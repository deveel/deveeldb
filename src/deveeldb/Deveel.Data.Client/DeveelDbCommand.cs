// 
//  Copyright 2010  Deveel
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
using System.Text;

using Deveel.Data.Protocol;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommand : DbCommand, ICloneable {

		/// <summary>
		/// The <see cref="DbConnection"/> object for this statement.
		/// </summary>
		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;

		/// <summary>
		/// The list of all <see cref="ResultSet"/> objects that represents the 
		/// results of a query.
		/// </summary>
		private ResultSet[] resultSetList;


		// private int max_field_size;
		private int maxRowCount;
		private int fetchSize = DefaultFetchSize;
		private int? commandTimeout;

		/// <summary>
		/// The list of streamable objects created via the <see cref="CreateStreamableObject"/> method.
		/// </summary>
		private List<StreamableObject> streamableObjectList;

		/// <summary>
		/// For multiple result sets, the index of the result set we are currently on.
		/// </summary>
		private int multiResultSetIndex;

		private SqlQuery[] queries;
		private string commandText;

		private DeveelDbParameterCollection parameters;
		private DeveelDbDataReader reader;

		public const int MaximumFetchSize = 512;
		public const int DefaultFetchSize = 32;

		public DeveelDbCommand() {
			DesignTimeVisible = true;
			parameters = new DeveelDbParameterCollection(this);
		}

		public DeveelDbCommand(string text)
			: this() {
			CommandText = text;
		}

		public DeveelDbCommand(string text, DeveelDbConnection connection)
			: this(text) {
			Connection = connection;
		}

		public DeveelDbCommand(string text, DeveelDbConnection connection, DeveelDbTransaction transaction)
			: this(text, connection) {
			Transaction = transaction;
		}

		/// <summary>
		/// Returns an array of <see cref="ResultSet"/> objects of the give 
		/// length for this statement.
		/// </summary>
		/// <param name="count"></param>
		/// <remarks>
		/// This is intended for multiple result queries (such as batch statements).
		/// </remarks>
		/// <returns></returns>
		internal ResultSet[] InternalResultSetList(int count) {
			if (count <= 0)
				throw new ArgumentException("'count' must be > 0");

			if (resultSetList != null && resultSetList.Length != count) {
				// Dispose all the ResultSet objects currently open.
				for (int i = 0; i < resultSetList.Length; ++i)
					resultSetList[i].Dispose();
				resultSetList = null;
			}

			if (resultSetList == null) {
				resultSetList = new ResultSet[count];
				for (int i = 0; i < count; ++i)
					resultSetList[i] = new ResultSet(connection);
			}

			return resultSetList;
		}

		/// <summary>
		/// Returns the single <see cref="ResultSet"/> object for this statement.
		/// </summary>
		/// <remarks>
		/// This should only be used for single result commands.
		/// </remarks>
		/// <returns></returns>
		internal ResultSet InternalResultSet() {
			return InternalResultSetList(1)[0];
		}

		/// <summary>
		/// Generates a new <see cref="StreamableObject"/> and stores it in the hold for 
		/// future access by the server.
		/// </summary>
		/// <param name="x"></param>
		/// <param name="length"></param>
		/// <param name="type"></param>
		/// <returns></returns>
		internal StreamableObject CreateStreamableObject(Stream x, int length, ReferenceType type) {
			StreamableObject sOb = connection.CreateStreamableObject(x, length, type);
			if (streamableObjectList == null) {
				streamableObjectList = new List<StreamableObject>();
			}
			streamableObjectList.Add(sOb);
			return sOb;
		}

		internal bool NextResult() {
			// If we are at the end then return false
			if (resultSetList == null ||
				multiResultSetIndex >= resultSetList.Length) {
				return false;
			}

			// Move to the next result set.
			++multiResultSetIndex;

			// We successfully moved to the next result
			return true;
		}

		internal ResultSet ResultSet {
			get {
				if (resultSetList != null) {
					if (multiResultSetIndex < resultSetList.Length) {
						return resultSetList[multiResultSetIndex];
					}
				}
				return null;
			}
		}

		internal int UpdateCount {
			get {
				if (resultSetList != null) {
					if (multiResultSetIndex < resultSetList.Length) {
						ResultSet rs = resultSetList[multiResultSetIndex];
						if (rs.IsUpdate) {
							return rs.ToInteger();
						}
					}
				}
				return -1;
			}
		}

		internal DeveelDbLob GetLob(object obj) {
			if (obj is StreamableObject) {
				var sOb = (StreamableObject)obj;
				if (sOb.Type == ReferenceType.Binary)
					return new DeveelDbLob(connection, ResultSet.result_id, sOb);
			} else if (obj is ByteLongObject) {
				var blob = (ByteLongObject) obj;
				return new DeveelDbLob(this, blob.GetInputStream(), ReferenceType.Binary, blob.Length);
			} else if (obj is StringObject) {
				var clob = (StringObject) obj;
				byte[] bytes = Encoding.Unicode.GetBytes(clob.ToString());
				return new DeveelDbLob(this, new MemoryStream(bytes), ReferenceType.UnicodeText, bytes.Length);
			}

			throw new InvalidOperationException("Unable to convert into a LOB.");
		}


		/// <summary>
		/// Casts an internal object to the sql_type given for return by methods
		/// such as GetValue
		/// </summary>
		/// <param name="ob"></param>
		/// <param name="sqlType"></param>
		/// <returns></returns>
		internal Object ObjectCast(Object ob, SqlType sqlType) {
			switch (sqlType) {
				case (SqlType.Bit):
					return ob;
				case (SqlType.TinyInt):
					return ((BigNumber)ob).ToByte();
				case (SqlType.SmallInt):
					return ((BigNumber)ob).ToInt16();
				case (SqlType.Integer):
					return ((BigNumber)ob).ToInt32();
				case (SqlType.BigInt):
					return ((BigNumber)ob).ToInt64();
				case (SqlType.Float):
					return ((BigNumber)ob).ToDouble();
				case (SqlType.Real):
					return ((BigNumber)ob).ToSingle();
				case (SqlType.Double):
					return ((BigNumber)ob).ToDouble();
				case (SqlType.Numeric):
					return ((BigNumber)ob).ToBigDecimal();
				case (SqlType.Decimal):
					return ((BigNumber)ob).ToBigDecimal();
				case (SqlType.Char):
					return MakeString(ob);
				case (SqlType.VarChar):
					return MakeString(ob);
				case (SqlType.LongVarChar):
					return MakeString(ob);
				case (SqlType.Date):
				case (SqlType.Time):
				case (SqlType.TimeStamp):
					return (DateTime)ob;
				case (SqlType.Binary):
				// fall through
				case (SqlType.VarBinary):
				// fall through
				case (SqlType.LongVarBinary):
					DeveelDbLob lob = GetLob(ob);
					return lob.GetBytes(0, (int) lob.Length);
				case (SqlType.Null):
					return ob;
				case (SqlType.Other):
					return ob;
				case (SqlType.Object):
					return ob;
				case (SqlType.Distinct):
					// (Not supported)
					return ob;
				case (SqlType.Struct):
					// (Not supported)
					return ob;
				case (SqlType.Array):
					// (Not supported)
					return ob;
				case (SqlType.Blob):
					// return AsBlob(ob);
				case (SqlType.Clob):
					// return AsClob(ob);
					return GetLob(ob);
				case (SqlType.Ref):
					// (Not supported)
					return ob;
				default:
					return ob;
			}
		}

		/// <summary>
		/// If the object represents a String or is a form that can be readily 
		/// translated to a <see cref="String"/> (such as a <see cref="IClob"/>, 
		/// <see cref="String"/>, <see cref="BigNumber"/>, <see cref="Boolean"/>, etc) 
		/// the string representation of the given <see cref="object"/> is returned.
		/// </summary>
		/// <param name="ob"></param>
		/// <remarks>
		/// This method is a convenient way to convert objects such as <see cref="IClob"/>s 
		/// into <see cref="string"/> objects. This will cause a <see cref="InvalidCastException"/>
		/// if the given object represents a BLOB or <see cref="ByteLongObject"/>.
		/// </remarks>
		/// <returns></returns>
		internal String MakeString(Object ob) {
			if (ob is StreamableObject) {
				DeveelDbLob clob = GetLob(ob);
				long clobLen = clob.Length;
				if (clobLen < 16384L * 65536L) {
					return clob.GetString(0, (int)clobLen);
				}
				throw new DataException("IClob too large to return as a string.");
			}
			if (ob is ByteLongObject)
				throw new InvalidCastException();

			return ob.ToString();
		}

		/// <summary>
		/// Executes the given <see cref="SqlQuery"/> object and fill's in at 
		/// most the top 10 entries of the result set.
		/// </summary>
		/// <returns></returns>
		internal ResultSet[] ExecuteQuery() {
			if (connection == null)
				throw new InvalidOperationException("The connection was not set.");
			if (connection.State == ConnectionState.Closed)
				throw new InvalidOperationException("The connection is closed.");

			if (queries == null)
				throw new InvalidOperationException("The command text was not set.");

			// Allocate the result set for this batch
			ResultSet[] results = InternalResultSetList(queries.Length);

			// Reset the result set index
			multiResultSetIndex = 0;

			// For each query,
			foreach (ResultSet resultSet in results)
				resultSet.CloseCurrentResult();

			// Execute each query
			connection.ExecuteQueries(queries, results);

			// Post processing on the ResultSet objects
			for (int i = 0; i < queries.Length; ++i) {
				ResultSet resultSet = results[i];
				// Set the fetch size
				resultSet.SetFetchSize(fetchSize);
				// Set the max row count
				resultSet.SetMaxRowCount(maxRowCount);
				// Does the result set contain large objects?  We can't cache a
				// result that contains binary data.
				bool containsLargeObjects = resultSet.ContainsLargeObjects();
				// If the result row count < 40 then download and store locally in the
				// result set and dispose the resources on the server.
				if (!containsLargeObjects && resultSet.RowCount < 40) {
					resultSet.StoreResultLocally();
				} else {
					resultSet.UpdateResultPart(0, System.Math.Min(10, resultSet.RowCount));
				}
			}

			return results;
		}

		//TODO: move to connection string...
		public int MaxRowCount {
			get { return maxRowCount; }
			set {
				if (value >= 0) {
					maxRowCount = value;
				} else {
					throw new DataException("MaxRows negative.");
				}
			}
		}

		public int FetchSize {
			get { return fetchSize; }
			set {
				if (value > MaximumFetchSize)
					throw new ArgumentOutOfRangeException();
				fetchSize = value;
			}
		}

		private void AssertConnectionOpen() {
			if (connection == null)
				throw new InvalidOperationException("No connection was not set.");
			if (connection.State != ConnectionState.Open)
				throw new InvalidOperationException("The underlying connection is not open.");
		}

		private void VerifyParameters() {
			parameters.VerifyParameterNames(connection.Settings.ParameterStyle);
		}


		protected override void Dispose(bool disposing) {
			if (disposing) {
				try {
					// Behaviour of calls to Statement undefined after this method finishes.
					if (resultSetList != null) {
						foreach (ResultSet resultSet in resultSetList) {
							resultSet.Dispose();
						}
						resultSetList = null;
					}
					// Remove any streamable objects that have been created on the client
					// side.
					if (streamableObjectList != null) {
						foreach (StreamableObject streamableObject in streamableObjectList) {
							connection.RemoveStreamableObject(streamableObject);
						}
						streamableObjectList = null;
					}
				} catch (DataException) {
				}		
			}
		}

		#region Implementation of IDbCommand

		public override void Prepare() {
			AssertConnectionOpen();

			if (queries == null || parameters.Count == 0)
				return;

			//TODO: we should handle this better: it's nasty that a set
			//      of parameters are set for all the queries...

			foreach (SqlQuery query in queries) {
				for (int j = 0; j < parameters.Count; j++) {
					DeveelDbParameter parameter = parameters[j];
					if (parameter.Value is DeveelDbLob) {
						query.SetVariable(j, ((DeveelDbLob)parameter.Value).ObjectRef);
					} else if (parameter.Value is Stream) {
						var stream = (Stream) parameter.Value;
						if (parameter.LongSize > 8*1024) {
							var lob = new DeveelDbLob(this, stream, parameter.ReferenceType, parameter.LongSize, true);
							query.SetVariable(j, lob.ObjectRef);
						} else {
							if (parameter.ReferenceType == ReferenceType.Binary) {
								query.SetVariable(j, new ByteLongObject(stream, parameter.Size));
							} else if (parameter.ReferenceType == ReferenceType.AsciiText) {
								var sb = new StringBuilder();
								for (int k = 0; k < parameter.Size; ++k) {
									int v = stream.ReadByte();
									if (v == -1)
										throw new IOException("Premature EOF reached.");
									sb.Append((char) v);
								}
								query.SetVariable(j, StringObject.FromString(sb.ToString()));
							} else {
								var sb = new StringBuilder();
								int halfLength = parameter.Size/2;
								for (int k = 0; k < halfLength; ++k) {
									int v1 = stream.ReadByte();
									int v2 = stream.ReadByte();
									if (v1 == -1 || v2 == -1)
										throw new IOException("Premature EOF reached.");

									sb.Append((char) ((v1 << 8) + v2));
								}

								query.SetVariable(j, StringObject.FromString(sb.ToString()));
							}
						}
					} else {
						query.SetVariable(j, CastHelper.CastToSQLType(parameter.Value, parameter.SqlType, parameter.Size, parameter.Scale));
					}
				}
			}
		}

		public override void Cancel() {
			if (resultSetList != null) {
				try {
					foreach (ResultSet resultSet in resultSetList) {
						connection.DisposeResult(resultSet.ResultId);
					}
				} catch (Exception) {
				}
			}
		}

		public new DeveelDbParameter CreateParameter() {
			return new DeveelDbParameter();
		}

		protected override DbParameter CreateDbParameter() {
			return CreateParameter();
		}

		public override int ExecuteNonQuery() {
			AssertConnectionOpen();
			VerifyParameters();

			connection.SetState(ConnectionState.Executing);

			try {
				ResultSet[] resultSet = ExecuteQuery();
				if (resultSet.Length > 1)
					throw new InvalidOperationException();

				return !resultSet[0].IsUpdate ? -1 : resultSet[0].ToInteger();
			} finally {
				connection.EndState();
			}
		}

		public new DeveelDbDataReader ExecuteReader() {
			AssertConnectionOpen();
			VerifyParameters();

			if (reader != null)
				throw new InvalidOperationException("A reader is already opened for this command.");

			if (connection.State == ConnectionState.Fetching)
				throw new InvalidOperationException("The connection is already busy fetching data.");

			connection.SetState(ConnectionState.Fetching);

			try {
				ExecuteQuery();
				reader = new DeveelDbDataReader(this);
				reader.Closed += new EventHandler(ReaderClosed);
			} catch(Exception) {
				connection.EndState();
				throw;
			}

			return reader;
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			//TODO: support behaviors...
			if (behavior != CommandBehavior.Default)
				throw new ArgumentException("Behavior '" + behavior + "' not supported (yet).");

			return ExecuteReader();
		}

		private void ReaderClosed(object sender, EventArgs e) {
			connection.EndState();
			reader = null;
		}

		public override object ExecuteScalar() {
			AssertConnectionOpen();
			VerifyParameters();

			connection.SetState(ConnectionState.Executing);

			try {
				ResultSet[] result = ExecuteQuery();
				if (result.Length != 1)
					return null;

				if (!result[0].First())
					return null;

				object value = result[0].GetRawColumn(0);
				if (value == null)
					return DBNull.Value;

				if (connection.Settings.StrictGetValue) {
					// Convert depending on the column type,
					ColumnDescription colDesc = result[0].GetColumn(0);
					SqlType sqlType = colDesc.SQLType;

					return ObjectCast(value, sqlType);
				}

				// We don't support blobs in a scalar.
				if (value is ByteLongObject ||
					value is StreamableObject) {
					throw new DataException();
				}

				return value;
			} finally {
				connection.EndState();
			}
		}

		public override bool DesignTimeVisible { get; set; }

		public new DeveelDbConnection Connection {
			get { return connection; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				if (connection != value)
					Transaction = null;

				connection = value;
				transaction = connection.currentTransaction;
			}
		}

		protected override DbConnection DbConnection {
			get { return Connection; }
			set {
				if (!(value is DeveelDbConnection))
					throw new ArgumentException("Trying to set a connection that is not a '" + typeof(DeveelDbConnection) + "'.");

				Connection = (DeveelDbConnection) value;
			}
		}

		protected override DbTransaction DbTransaction {
			get { return Transaction; }
			set {
				if (!(value is DeveelDbTransaction))
					throw new ArgumentException("Trying to set a transaction that is not a '" + typeof(DeveelDbTransaction) + "'.");

				Transaction = (DeveelDbTransaction) value;
			}
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

		public override string CommandText {
			get { return commandText; }
			set {
				if (value != null) {
					var style = ParameterStyle.Marker;
					if (connection != null)
						style = connection.Settings.ParameterStyle;

					commandText = value;
					queries = new SqlQuery[] {new SqlQuery(value, style)};
				} else {
					queries = null;
					commandText = null;
				}

				parameters.Clear();
			}
		}

		public override int CommandTimeout {
			get {
				if (commandTimeout != null)
					return commandTimeout.Value;
				if (connection != null)
					return connection.Settings.QueryTimeout;
				return -1;
			}
			set {
				if (value < 0)
					throw new ArgumentException("Cannot set a timeout value less than 0");

				commandTimeout = value;
			}
		}

		public override CommandType CommandType {
			get { return CommandType.Text; }
			set {
				if (value != CommandType.Text)
					throw new ArgumentException();
			}
		}

		protected override DbParameterCollection DbParameterCollection {
			get { return Parameters; }
		}

		public new DeveelDbParameterCollection Parameters {
			get {
				if (parameters == null)
					parameters = new DeveelDbParameterCollection(this);
				return parameters;
			}
		}

		//TODO: currently not supported...
		public override UpdateRowSource UpdatedRowSource {
			get { return UpdateRowSource.None; }
			set {
				if (value != UpdateRowSource.None)
					throw new ArgumentException();
			}
		}

		#endregion

		public object Clone() {
			DeveelDbCommand command = new DeveelDbCommand((string)commandText.Clone(), connection, transaction);
			command.CommandTimeout = CommandTimeout;
			foreach(DeveelDbParameter parameter in Parameters)
				command.Parameters.Add((DeveelDbParameter) parameter.Clone());
			return command;
		}
	}
}