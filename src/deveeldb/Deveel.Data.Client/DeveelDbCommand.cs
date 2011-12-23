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
using System.Collections;
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
		private ResultSet[] result_set_list;


		private int max_field_size;
		private int max_row_count;
		private int fetch_size = DefaultFetchSize;
		private bool designTimeVisible = true;
		private bool timeoutWasSet;
		private int commandTimeout;

		/// <summary>
		/// The list of _queries to execute in a batch.
		/// </summary>
		private ArrayList batch_list;

		/// <summary>
		/// The list of streamable objects created via the <see cref="CreateStreamableObject"/> method.
		/// </summary>
		private ArrayList streamable_object_list;

		/// <summary>
		/// For multiple result sets, the index of the result set we are currently on.
		/// </summary>
		private int multi_result_set_index;

		private SqlQuery[] _queries;
		private string commandText;

		private DeveelDbParameterCollection parameters;
		private DeveelDbDataReader reader;

		public const int MaximumFetchSize = 512;
		public const int DefaultFetchSize = 32;

		public DeveelDbCommand() {
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
		/// This is intended for multiple result _queries (such as batch statements).
		/// </remarks>
		/// <returns></returns>
		internal ResultSet[] InternalResultSetList(int count) {
			if (count <= 0)
				throw new ArgumentException("'count' must be > 0");

			if (result_set_list != null && result_set_list.Length != count) {
				// Dispose all the ResultSet objects currently open.
				for (int i = 0; i < result_set_list.Length; ++i)
					result_set_list[i].Dispose();
				result_set_list = null;
			}

			if (result_set_list == null) {
				result_set_list = new ResultSet[count];
				for (int i = 0; i < count; ++i)
					result_set_list[i] = new ResultSet(connection);
			}

			return result_set_list;
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
			StreamableObject s_ob = connection.CreateStreamableObject(x, length, type);
			if (streamable_object_list == null) {
				streamable_object_list = new ArrayList();
			}
			streamable_object_list.Add(s_ob);
			return s_ob;
		}

		internal bool NextResult() {
			// If we are at the end then return false
			if (result_set_list == null ||
				multi_result_set_index >= result_set_list.Length) {
				return false;
			}

			// Move to the next result set.
			++multi_result_set_index;

			// We successfully moved to the next result
			return true;
		}

		internal ResultSet ResultSet {
			get {
				if (result_set_list != null) {
					if (multi_result_set_index < result_set_list.Length) {
						return result_set_list[multi_result_set_index];
					}
				}
				return null;
			}
		}

		internal int UpdateCount {
			get {
				if (result_set_list != null) {
					if (multi_result_set_index < result_set_list.Length) {
						ResultSet rs = result_set_list[multi_result_set_index];
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
				StreamableObject s_ob = (StreamableObject)obj;
				if (s_ob.Type == ReferenceType.Binary) {
					return new DeveelDbLob(connection, ResultSet.result_id, s_ob);
				}
			} else if (obj is ByteLongObject) {
				ByteLongObject blob = (ByteLongObject) obj;
				return new DeveelDbLob(this, blob.GetInputStream(), ReferenceType.Binary, blob.Length);
			} else if (obj is StringObject) {
				StringObject clob = (StringObject) obj;
				byte[] bytes = Encoding.Unicode.GetBytes(clob.ToString());
				return new DeveelDbLob(this, new MemoryStream(bytes), ReferenceType.UnicodeText, bytes.Length);
			}

			throw new InvalidOperationException("Unable to convert into a LOB.");
		}


		/// <summary>
		/// Casts an internal object to the sql_type given for return by methods
		/// such as <see cref="GetValue"/>.
		/// </summary>
		/// <param name="ob"></param>
		/// <param name="sql_type"></param>
		/// <returns></returns>
		internal Object ObjectCast(Object ob, SqlType sql_type) {
			switch (sql_type) {
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
				long clob_len = clob.Length;
				if (clob_len < 16384L * 65536L) {
					return clob.GetString(0, (int)clob_len);
				}
				throw new DataException("IClob too large to return as a string.");
			} else if (ob is ByteLongObject) {
				throw new InvalidCastException();
			} else {
				return ob.ToString();
			}
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

			if (_queries == null)
				throw new InvalidOperationException("The command text was not set.");

			// Allocate the result set for this batch
			ResultSet[] results = InternalResultSetList(_queries.Length);

			// Reset the result set index
			multi_result_set_index = 0;

			// For each query,
			for (int i = 0; i < results.Length; ++i)
				// Make sure the result set is closed
				results[i].CloseCurrentResult();

			// Execute each query
			connection.ExecuteQueries(_queries, results);

			// Post processing on the ResultSet objects
			for (int i = 0; i < _queries.Length; ++i) {
				ResultSet result_set = results[i];
				// Set the fetch size
				result_set.SetFetchSize(fetch_size);
				// Set the max row count
				result_set.SetMaxRowCount(max_row_count);
				// Does the result set contain large objects?  We can't cache a
				// result that contains binary data.
				bool contains_large_objects = result_set.ContainsLargeObjects();
				// If the result row count < 40 then download and store locally in the
				// result set and dispose the resources on the server.
				if (!contains_large_objects && result_set.RowCount < 40) {
					result_set.StoreResultLocally();
				} else {
					result_set.UpdateResultPart(0, System.Math.Min(10, result_set.RowCount));
				}
			}

			return results;
		}

		//TODO: move to connection string...
		public int MaxRowCount {
			get { return max_row_count; }
			set {
				if (value >= 0) {
					max_row_count = value;
				} else {
					throw new DataException("MaxRows negative.");
				}
			}
		}

		public int FetchSize {
			get { return fetch_size; }
			set {
				if (value > MaximumFetchSize)
					throw new ArgumentOutOfRangeException();
				fetch_size = value;
			}
		}


		#region Implementation of IDisposable

		public void Dispose() {
			try {
				// Behaviour of calls to Statement undefined after this method finishes.
				if (result_set_list != null) {
					for (int i = 0; i < result_set_list.Length; ++i) {
						result_set_list[i].Dispose();
					}
					result_set_list = null;
				}
				// Remove any streamable objects that have been created on the client
				// side.
				if (streamable_object_list != null) {
					int sz = streamable_object_list.Count;
					for (int i = 0; i < sz; ++i) {
						StreamableObject s_object =
									   (StreamableObject)streamable_object_list[i];
						connection.RemoveStreamableObject(s_object);
					}
					streamable_object_list = null;
				}
			} catch (DataException) {
			}
		}

		#endregion

		#region Implementation of IDbCommand

		public override void Prepare() {
			if (_queries == null || parameters.Count == 0)
				return;

			//TODO: we should handle this better: it's nasty that a set
			//      of parameters are set for all the _queries...

			for (int i = 0; i < _queries.Length; i++) {
				SqlQuery query = _queries[i];
				for (int j = 0; j < parameters.Count; j++) {
					DeveelDbParameter parameter = parameters[j];
					if (parameter.Value is DeveelDbLob) {
						query.SetVariable(j, ((DeveelDbLob)parameter.Value).ObjectRef);
					} else if (parameter.Value is Stream) {
						Stream stream = (Stream) parameter.Value;
						if (parameter.LongSize > 8 * 1024) {
							DeveelDbLob lob = new DeveelDbLob(this, stream, parameter.ReferenceType, parameter.LongSize, true);
							query.SetVariable(j, lob.ObjectRef);
						} else {
							if (parameter.ReferenceType == ReferenceType.Binary) {
								query.SetVariable(j, new ByteLongObject(stream, parameter.Size));
							} else if (parameter.ReferenceType == ReferenceType.AsciiText) {
								StringBuilder sb = new StringBuilder();
								for (int k = 0; k < parameter.Size; ++k) {
									int v = stream.ReadByte();
									if (v == -1)
										throw new IOException("Premature EOF reached.");
									sb.Append((char)v);
								}
								query.SetVariable(j, StringObject.FromString(sb.ToString()));
							} else {
								StringBuilder sb = new StringBuilder();
								int halfLength = parameter.Size/2;
								for (int k = 0; k < halfLength; ++k) {
									int v1 = stream.ReadByte();
									int v2 = stream.ReadByte();
									if (v1 == -1 || v2 == -1)
										throw new IOException("Premature EOF reached.");

									sb.Append((char)((v1 << 8) + v2));
								}

								query.SetVariable(j, StringObject.FromString(sb.ToString()));
							}
						}
					}else if (!(parameter.Value is DeveelDbLob))
						query.SetVariable(j, CastHelper.CastToSQLType(parameter.Value, parameter.SqlType, parameter.Size, parameter.Scale));
				}
			}
		}

		public override void Cancel() {
			if (result_set_list != null) {
				for (int i = 0; i < result_set_list.Length; ++i) {
					connection.DisposeResult(result_set_list[i].ResultId);
				}
			}
		}

		public new DeveelDbParameter CreateParameter() {
			DeveelDbParameter parameter = new DeveelDbParameter();
			parameter.paramStyle = connection.Settings.ParameterStyle;
			return parameter;
		}

		protected override DbParameter CreateDbParameter() {
			return CreateParameter();
		}

		public override int ExecuteNonQuery() {
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
			connection.SetState(ConnectionState.Executing);

			try {
				ResultSet[] result = ExecuteQuery();
				if (result.Length > 1)
					throw new InvalidOperationException();

				if (result[0].RowCount > 1)
					throw new DataException("The result of the query returned more than one row and cannot be a scalar.");
				if (result[0].ColumnCount > 1)
					throw new DataException("The result of the query has more than one column and cannot be a scalar.");

				if (!result[0].First())
					return null;

				Object ob = result[0].GetRawColumn(0);
				if (ob == null)
					return ob;

				if (connection.Settings.StrictGetValue) {
					// Convert depending on the column type,
					ColumnDescription col_desc = result[0].GetColumn(0);
					SqlType sql_type = col_desc.SQLType;

					return ObjectCast(ob, sql_type);

				}
				// We don't support blobs in a scalar.
				if (ob is ByteLongObject ||
					ob is StreamableObject) {
					throw new DataException();
				}

				return ob;
			} finally {
				connection.EndState();
			}
		}

		public override bool DesignTimeVisible {
			get { return designTimeVisible; }
			set { designTimeVisible = value;
			}
		}

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
					throw new ArgumentException();

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
					ParameterStyle style = ParameterStyle.Marker;
					if (connection != null)
						style = connection.Settings.ParameterStyle;

					commandText = value;
					_queries = new SqlQuery[] {new SqlQuery(value, style)};
				} else {
					_queries = null;
					commandText = null;
				}

				parameters.Clear();
			}
		}

		public override int CommandTimeout {
			get {
				if (timeoutWasSet)
					return commandTimeout;
				if (connection != null)
					return connection.Settings.QueryTimeout;
				return -1;
			}
			set {
				if (value < 0) {
					timeoutWasSet = false;
					commandTimeout = -1;
				} else {
					commandTimeout = value;
					timeoutWasSet = true;
				}
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