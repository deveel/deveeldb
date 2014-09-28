// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;

using Deveel.Data.Protocol;
using Deveel.Data.Sql;

using SysParameterDirection = System.Data.ParameterDirection;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommand : DbCommand {
		private int? timeout;
		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;

		private bool prepared;
		private Dictionary<string, object> preparedParameters;

		private readonly DeveelDbParameterCollection parameters;

		private int resultIndex;
		private LocalQueryResult[] results;

		public DeveelDbCommand() 
			: this((DeveelDbConnection) null) {
		}

		public DeveelDbCommand(DeveelDbConnection connection) 
			: this(connection, null) {
		}

		public DeveelDbCommand(string commandText) 
			:this(null, commandText) {
		}

		public DeveelDbCommand(DeveelDbConnection connection, string commandText) {
			Connection = connection;
			CommandText = commandText;

			parameters = new DeveelDbParameterCollection(this);
		}

		public override void Prepare() {
			throw new NotImplementedException();
		}

		public override string CommandText { get; set; }

		public override int CommandTimeout {
			get {
				if (timeout == null && connection != null)
					return connection.Settings.QueryTimeout;
				if (timeout == null)
					return -1;

				return timeout.Value;
			}
			set { timeout = value; }
		}

		public override CommandType CommandType {
			get { return CommandType.Text; }
			set {
				if (value != CommandType.Text)
					throw new ArgumentException();
			}
		}

		public override UpdateRowSource UpdatedRowSource { get; set; }

		protected override DbConnection DbConnection {
			get { return connection; }
			set {
				connection = (DeveelDbConnection) value;

				if (connection != null &&
				    timeout == null)
					timeout = connection.Settings.QueryTimeout;
			}
		}

		public new DeveelDbConnection Connection {
			get { return (DeveelDbConnection) DbConnection; }
			set { DbConnection = value; }
		}

		protected override DbParameterCollection DbParameterCollection {
			get { return Parameters; }
		}

		public new DeveelDbParameterCollection Parameters {
			get { return parameters; }
		}

		protected override DbTransaction DbTransaction {
			get { return Transaction; }
			set { Transaction = (DeveelDbTransaction) value; }
		}

		public new DeveelDbTransaction Transaction {
			get { return transaction; }
			set {
				if (value == null && transaction != null)
					transaction = null;
				else if (transaction != null && 
					(value != null && value.Id != transaction.Id))
					throw new ArgumentException();

				transaction = value;
			}
		}

		public override bool DesignTimeVisible { get; set; }

		internal bool NextResult() {
			// If we are at the end then return false
			if (results == null ||
				resultIndex+1 >= results.Length) {
				return false;
			}

			// Move to the next result set.
			++resultIndex;

			// We successfully moved to the next result
			return true;
		}

		internal LocalQueryResult CurrentResult {
			get {
				if (results != null) {
					if (resultIndex < results.Length) {
						return results[resultIndex];
					}
				}
				return null;
			}
		}

		public override void Cancel() {
			try {
				if (results != null) {
					foreach (var result in results) {
						connection.DisposeResult(result.ResultId);
					}
				}
			} finally {
				connection.EndState();
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (results != null) {
					foreach (var result in results) {
						result.Dispose();
					}

					results = null;
				}
			}
		}

		protected override DbParameter CreateDbParameter() {
			return CreateParameter();
		}

		public new DeveelDbParameter CreateParameter() {
			return new DeveelDbParameter();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			return ExecuteReader(behavior);
		}

		public new DeveelDbDataReader ExecuteReader(CommandBehavior behavior) {
			try {
				if (connection.State == ConnectionState.Fetching)
					throw new InvalidOperationException("Another reader is already open for the connection.");

				if (connection.State != ConnectionState.Open)
					throw new InvalidOperationException("The connection is not open.");

				ExecuteQuery();

				return new DeveelDbDataReader(this, behavior);
			} catch (Exception) {
				
				throw;
			}
		}

		public new DeveelDbDataReader ExecuteReader() {
			return ExecuteReader(CommandBehavior.Default);
		}

		public override int ExecuteNonQuery() {
			try {
				connection.ChangeState(ConnectionState.Executing);

				ExecuteQuery();

				if (results == null || results.Length == 0)
					return -1;

				var result = results[0];
				if (!result.IsUpdate)
					return -1;

				return result.AffectedRows;
			} catch (Exception) {
				//TODO: throw a specialized exception ...
				throw;
			} finally {
				connection.EndState();
			}
		}

		public override object ExecuteScalar() {
			try {
				connection.ChangeState(ConnectionState.Executing);

				ExecuteQuery();

				if (results == null || results.Length == 0)
					return null;

				var result = results[0];
				if (!result.First())
					return null;

				if (result.RowCount == 0)
					return null;

				return result.GetRuntimeValue(0);
			} catch (Exception) {

				throw;
			} finally {
				connection.EndState();
			}
		}

		private SqlQuery CreateQuery() {
			SqlQueryParameter[] queryParameters;
			if (prepared && preparedParameters != null) {
				queryParameters = new SqlQueryParameter[preparedParameters.Count];

				int index = -1;
				foreach (var parameter in preparedParameters) {
					var name = parameter.Key;
					if (String.IsNullOrEmpty(name))
						name = SqlQueryParameter.MarkerName;

					queryParameters[++index] = new SqlQueryParameter(name, parameter.Value);
				}
			} else {
				queryParameters = new SqlQueryParameter[parameters.Count];
				for (int i = 0; i < parameters.Count; i++) {
					var parameter = parameters[i];
					var name = parameter.ParameterName;
					if (String.IsNullOrEmpty(name))
						name = SqlQueryParameter.MarkerName;

					var queryParameter = new SqlQueryParameter(name, parameter.Value);
					queryParameter.Size = parameter.Size;
					queryParameter.Direction = parameter.Direction == SysParameterDirection.Input
						? ParameterDirection.Input
						: ParameterDirection.Output;
					queryParameter.SqlType = parameter.SqlType;
					queryParameters[i] = queryParameter;
				}
			}

			var query = new SqlQuery(CommandText);

			// now verify all parameter names are consistent
			foreach (var parameter in queryParameters) {
				if (connection.Settings.ParameterStyle == ParameterStyle.Marker) {
					if (!String.IsNullOrEmpty(parameter.Name) && 
						parameter.Name != SqlQueryParameter.MarkerName)
						throw new InvalidOperationException();
				} else if (connection.Settings.ParameterStyle == ParameterStyle.Named) {
					if (String.IsNullOrEmpty(parameter.Name))
						throw new InvalidOperationException("Named parameters must have a name defined.");

					if (parameter.Name == SqlQueryParameter.MarkerName)
						throw new InvalidOperationException();
					if (parameter.Name.Length <= 1)
						throw new InvalidOperationException();
					if (!Char.IsLetter(parameter.Name[0]) && 
						parameter.Name[0] != SqlQueryParameter.NamePrefix)
						throw new InvalidOperationException();
				}

				query.Parameters.Add(parameter);
			}

			return query;
		}

		private void ExecuteQuery() {
			var query = CreateQuery();

			UploadLargeObjects(query);

			var response = connection.ExecuteQuery(query);
			CreateResults(response);
		}

		private StreamableObject UploadStream(Stream stream) {
			var obj = connection.CreateStreamableObject(ReferenceType.Binary, stream.Length, ObjectPersistenceType.Volatile);
			var channel = connection.OpenObjectChannel(obj.Identifier, ObjectPersistenceType.Volatile);
			
			//TODO: copy the data from the stream to the channel ...

			return obj;
		}

		private void UploadLargeObjects(SqlQuery query) {
			foreach (var parameter in query.Parameters) {
				//TODO: support for blob ...

				if (parameter.Value is Stream) {
					var stream = (Stream) parameter.Value;
					var obj = UploadStream(stream);
					parameter.Value = new SqlQueryParameter(parameter.Name, obj);
				}

				// TODO: convert the value to a serializable
			}
		}

		private void CreateResults(IQueryResponse[] response) {
			results = new LocalQueryResult[response.Length];

			for (int i = 0; i < response.Length; i++) {
				var r = response[i];
				var columns = new QueryResultColumn[r.ColumnCount];
				for (int j = 0; j < columns.Length; j++) {
					columns[j] = r.GetColumnDescription(j);
				}

				var result = new LocalQueryResult(connection);
				result.QueryTime = r.QueryTimeMillis;

				result.Setup(r.ResultId, columns, r.RowCount);
				result.SetFetchSize(connection.Settings.FetchSize);
				result.SetMaxRowCount(connection.Settings.MaxFetchSize);

				// Does the result set contain large objects?  We can't cache a
				// result that contains binary data.

				bool hasLargeObject = result.HasLargeObject;
				
				// If the result row count < 40 then download and store locally in the
				// result set and dispose the resources on the server.
				if (!hasLargeObject && result.RowCount < 40) {
					result.DownloadAndClose();
				} else {
					result.Download(0, System.Math.Min(10, result.RowCount));
				}

				results[i] = result;
			}
		}
	}
}