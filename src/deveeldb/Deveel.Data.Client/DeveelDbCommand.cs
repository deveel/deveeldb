// 
//  Copyright 2010-2015 Deveel
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
//


using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;

using Deveel.Data.Protocol;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using SysParameterDirection = System.Data.ParameterDirection;
using System.Text;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommand : DbCommand {
		private int? timeout;
		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;

		private bool prepared;
		private List<QueryParameter> preparedParameters;

		private DeveelDbParameterCollection parameters;

		private int resultIndex;
		private LocalQueryResult[] results;

		public DeveelDbCommand()
			: this((DeveelDbConnection)null) {
		}

		public DeveelDbCommand(DeveelDbConnection connection)
			: this(connection, null) {
		}

		public DeveelDbCommand(string commandText)
			: this(null, commandText) {
		}

		public DeveelDbCommand(DeveelDbConnection connection, string commandText) {
			Connection = connection;
			CommandText = commandText;

			parameters = new DeveelDbParameterCollection(this);
		}

		private QueryParameterDirection GetParamDirection(SysParameterDirection direction) {
			if (direction == SysParameterDirection.Input)
				return QueryParameterDirection.In;
			if (direction == SysParameterDirection.Output)
				return QueryParameterDirection.Out;
			if (direction == SysParameterDirection.InputOutput)
				return QueryParameterDirection.InOut;

			throw new NotSupportedException();
		}

		private QueryParameter PrepareParameter(DeveelDbParameter parameter) {
			// TODO: If we have a Value that is a Stream object, upload it and get 
			//       back the object ID to replace the value

			if (parameter.SqlType == SqlTypeCode.Unknown)
				throw new ArgumentException("Cannot resolve Unknown SQL Type");

			var name = parameter.ParameterName;
			if (String.IsNullOrEmpty(name))
				name = QueryParameter.Marker;

			var meta = new [] {
				new DataTypeMeta("MaxSize", parameter.Size.ToString()),
				new DataTypeMeta("Precision", parameter.Precision.ToString()), 
				new DataTypeMeta("Scale", parameter.Scale.ToString()),
 				new DataTypeMeta("Locale", parameter.Locale) 
			};

			var dataType = SqlType.Resolve(parameter.SqlType, meta);
			var value = dataType.CreateFrom(parameter.Value);

			var queryParameter = new QueryParameter(name, dataType, value);
			queryParameter.Direction = GetParamDirection(parameter.Direction);
			return queryParameter;
		}

		private void ExecuteQuery() {
			var query = CreateQuery();

			int commitId = Transaction != null ? Transaction.CommitId : -1;
			var response = connection.ExecuteQuery(commitId, query);
			CreateResults(response);
		}

		private void CreateResults(IQueryResponse[] response) {
			results = new LocalQueryResult[response.Length];

			for (int i = 0; i < response.Length; i++) {
				var r = response[i];
				var columns = new QueryResultColumn[r.ColumnCount];
				for (int j = 0; j < columns.Length; j++) {
					columns[j] = r.GetColumn(j);
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

		private SqlQuery CreateQuery() {
			QueryParameter[] queryParameters;
			if (prepared && preparedParameters != null) {
				queryParameters = preparedParameters.ToArray();
			} else {
				queryParameters = new QueryParameter[parameters.Count];
				for (int i = 0; i < parameters.Count; i++) {
					var parameter = parameters[i];
					var queryParam = PrepareParameter(parameter);
					queryParameters[i] = queryParam;
				}
			}

			var query = new SqlQuery(CommandText);

			// now verify all parameter names are consistent
			foreach (var parameter in queryParameters) {
				if (connection.Settings.ParameterStyle == QueryParameterStyle.Marker) {
					if (!String.IsNullOrEmpty(parameter.Name) &&
						parameter.Name != QueryParameter.Marker)
						throw new InvalidOperationException();
				} else if (connection.Settings.ParameterStyle == QueryParameterStyle.Named) {
					if (String.IsNullOrEmpty(parameter.Name))
						throw new InvalidOperationException("Named parameters must have a name defined.");

					if (parameter.Name == QueryParameter.Marker)
						throw new InvalidOperationException();
					if (parameter.Name.Length <= 1)
						throw new InvalidOperationException();
					if (!Char.IsLetter(parameter.Name[0]) &&
						parameter.Name[0] != QueryParameter.NamePrefix)
						throw new InvalidOperationException();
				}

				query.Parameters.Add(parameter);
			}

			return query;
		}

		public override void Prepare() {
			if (!prepared) {
				try {
					PrepareCommand();
				} finally {
					prepared = true;
				}
			}
		}

		private void PrepareCommand() {
			
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

		public override CommandType CommandType { get; set; }

		public override UpdateRowSource UpdatedRowSource { get; set; }

		protected override DbConnection DbConnection {
			get { return Connection; }
			set { Connection = (DeveelDbConnection) value; }
		}

		public new DeveelDbConnection Connection {
			get { return connection; }
			set { connection = value; }
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
					(value != null && value.CommitId!= transaction.CommitId))
					throw new ArgumentException("The command is already bound to another transaction.");

				transaction = value;
			}
		}

		public override bool DesignTimeVisible { get; set; }

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

		internal bool NextResult() {
			// If we are at the end then return false
			if (results == null ||
				resultIndex + 1 >= results.Length) {
				return false;
			}

			// Move to the next result set.
			++resultIndex;

			// We successfully moved to the next result
			return true;
		}

		protected override DbParameter CreateDbParameter() {
			return CreateParameter();
		}

		public new DeveelDbParameter CreateParameter() {
			return new DeveelDbParameter();
		}

		private void AssertConnectionOpen() {
			if (Connection == null)
				throw new DeveelDbException("The command is not associated to any connection.");

			if (Connection.State == ConnectionState.Closed) {
				try {
					Connection.Open();
				} catch (DeveelDbException) {
					throw;
				} catch (Exception ex) {
					throw new DeveelDbException("Failed to open the underlying connection", ex);
				}
			}
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			return ExecuteReader(behavior);
		}

		public new DeveelDbDataReader ExecuteReader(CommandBehavior behavior) {
			try {
				AssertConnectionOpen();

				if (connection.State == ConnectionState.Fetching)
					throw new InvalidOperationException("Another reader is already open for the connection.");

				if (connection.State != ConnectionState.Open)
					throw new InvalidOperationException("The connection is not open.");

				ExecuteQuery();

				return new DeveelDbDataReader(this, behavior);
			} catch (Exception ex) {
				throw new DeveelDbException("An error occurred when executing the reader.", ex);
			}
		}

		public new DeveelDbDataReader ExecuteReader() {
			return ExecuteReader(CommandBehavior.Default);
		}

		public override int ExecuteNonQuery() {
			try {
				AssertConnectionOpen();

				connection.ChangeState(ConnectionState.Executing);

				ExecuteQuery();

				if (results == null || results.Length == 0)
					return -1;

				var result = results[0];
				if (!result.IsUpdate)
					return -1;

				return result.AffectedRows;
			} catch (Exception ex) {
				var message = new StringBuilder ();
				message.Append ("An error occurred while executing the non-query command");
				message.AppendLine ();
				message.Append(this.ToStringWithParameters());
				message.AppendLine ();
				message.Append(ex.ToString());
				throw new DeveelDbException(message.ToString(), ex);
			} finally {
				connection.EndState();
			}
		}

		public override object ExecuteScalar() {
			try {
				AssertConnectionOpen();

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
			} catch (Exception ex) {
				throw new DeveelDbException("Error when selecting a scalar value.", ex);
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
				}

				if (parameters != null) {
					foreach (IDbDataParameter parameter in parameters) {
						if (parameter.Value is IDisposable) {
							try {
								((IDisposable)parameter.Value).Dispose();
							} catch (Exception) {								
							}
						}
					}

					parameters.Clear();
				}

				if (preparedParameters != null) {
					foreach (var parameter in preparedParameters) {
						if (parameter.Value is IDisposable) {
							try {
								((IDisposable)parameter.Value).Dispose();
							} catch (Exception) {
							}
						}
					}
				}
			}

			preparedParameters = null;
			parameters = null;
			results = null;

			base.Dispose(disposing);
		}

		public string ToStringWithParameters()
		{
			int roughLengthEstimation = this.CommandText.Length + this.Parameters.Count * 64;
			var sb = new StringBuilder(roughLengthEstimation);
			sb.Append (this.CommandText + Environment.NewLine);
			for (int pi = 0; pi < this.Parameters.Count; ++pi)
			{
				var p = this.Parameters [pi];
				sb.AppendFormat ("ParameterName: {0},  Direction: {1}, DbType: {2}, Value: {3}" + Environment.NewLine, p.ParameterName, p.Direction, p.DbType, p.Value);
			}
			return sb.ToString ();
		}
	}
}
