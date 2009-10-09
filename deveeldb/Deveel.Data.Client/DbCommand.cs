//  
//  DbCommand.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Data;
using System.IO;

using Deveel.Math;

namespace Deveel.Data.Client {
	public sealed class DbCommand : IDbCommand {

		/// <summary>
		/// The <see cref="DbConnection"/> object for this statement.
		/// </summary>
		private DbConnection connection;
		private DbTransaction transaction;

		/// <summary>
		/// The list of all <see cref="ResultSet"/> objects that represents the 
		/// results of a query.
		/// </summary>
		private ResultSet[] result_set_list;


		private int max_field_size;
		private int max_row_count;
		private int query_timeout;
		private int fetch_size;

		private bool escape_processing;

		/// <summary>
		/// The list of queries to execute in a batch.
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

		private SQLQuery[] queries;
		private string commandText;

		private DbParameterCollection parameters;
		private DbDataReader reader;

		public DbCommand() {
			parameters = new DbParameterCollection();
			this.escape_processing = true;
		}

		public DbCommand(string text)
			: this() {
			CommandText = text;
		}

		public DbCommand(string text, DbConnection connection)
			: this(text) {
			Connection = connection;
		}

		public DbCommand(string text, DbConnection connection, DbTransaction transaction)
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
			if (count <= 0) {
				throw new ArgumentException("'count' must be > 0");
			}

			if (result_set_list != null && result_set_list.Length != count) {
				// Dispose all the ResultSet objects currently open.
				for (int i = 0; i < result_set_list.Length; ++i) {
					result_set_list[i].Dispose();
				}
				result_set_list = null;
			}

			if (result_set_list == null) {
				result_set_list = new ResultSet[count];
				for (int i = 0; i < count; ++i) {
					result_set_list[i] = new ResultSet(connection, this);
				}
			}

			return result_set_list;
		}

		/// <summary>
		/// Returns the single <see cref="ResultSet"/> object for this statement.
		/// </summary>
		/// <remarks>
		/// This should only be used for single result queries.
		/// </remarks>
		/// <returns></returns>
		internal ResultSet InternalResultSet() {
			return InternalResultSetList(1)[0];
		}

		/// <summary>
		/// Generates a new <see cref="Data.StreamableObject"/> and stores it in the hold for 
		/// future access by the server.
		/// </summary>
		/// <param name="x"></param>
		/// <param name="length"></param>
		/// <param name="type"></param>
		/// <returns></returns>
		internal Data.StreamableObject CreateStreamableObject(Stream x, int length, byte type) {
			Data.StreamableObject s_ob = connection.CreateStreamableObject(x, length, type);
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

		/// <summary>
		/// Returns the given object as a IBlob instance.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		internal IBlob AsBlob(Object ob) {
			if (ob is Data.StreamableObject) {
				Data.StreamableObject s_ob = (Data.StreamableObject)ob;
				byte type = (byte)(s_ob.Type & 0x0F);
				if (type == 2) {
					return new DbStreamableBlob(connection, ResultSet.result_id, type,
											   s_ob.Identifier, s_ob.Size);
				}
			} else if (ob is ByteLongObject) {
				return new DbBlob((ByteLongObject)ob);
			}
			throw new InvalidCastException();
		}

		/// <summary>
		/// Returns the given object as a IClob instance.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		internal IClob AsClob(Object ob) {
			if (ob is Data.StreamableObject) {
				Data.StreamableObject s_ob = (Data.StreamableObject)ob;
				byte type = (byte)(s_ob.Type & 0x0F);
				if (type == 3 ||
					type == 4) {
					return new DbStreamableClob(connection, ResultSet.result_id, type,
											   s_ob.Identifier, s_ob.Size);
				}
			} else if (ob is StringObject) {
				return new DbClob(ob.ToString());
			}
			throw new InvalidCastException();
		}

		/// <summary>
		/// Casts an internal object to the sql_type given for return by methods
		/// such as <see cref="GetValue"/>.
		/// </summary>
		/// <param name="ob"></param>
		/// <param name="sql_type"></param>
		/// <returns></returns>
		internal Object ObjectCast(Object ob, SQLTypes sql_type) {
			switch (sql_type) {
				case (SQLTypes.BIT):
					return ob;
				case (SQLTypes.TINYINT):
					return ((BigNumber)ob).ToByte();
				case (SQLTypes.SMALLINT):
					return ((BigNumber)ob).ToInt16();
				case (SQLTypes.INTEGER):
					return ((BigNumber)ob).ToInt32();
				case (SQLTypes.BIGINT):
					return ((BigNumber)ob).ToInt64();
				case (SQLTypes.FLOAT):
					return ((BigNumber)ob).ToDouble();
				case (SQLTypes.REAL):
					return ((BigNumber)ob).ToSingle();
				case (SQLTypes.DOUBLE):
					return ((BigNumber)ob).ToDouble();
				case (SQLTypes.NUMERIC):
					return ((BigNumber)ob).ToBigDecimal();
				case (SQLTypes.DECIMAL):
					return ((BigNumber)ob).ToBigDecimal();
				case (SQLTypes.CHAR):
					return MakeString(ob);
				case (SQLTypes.VARCHAR):
					return MakeString(ob);
				case (SQLTypes.LONGVARCHAR):
					return MakeString(ob);
				case (SQLTypes.DATE):
				case (SQLTypes.TIME):
				case (SQLTypes.TIMESTAMP):
					return (DateTime)ob;
				case (SQLTypes.BINARY):
				// fall through
				case (SQLTypes.VARBINARY):
				// fall through
				case (SQLTypes.LONGVARBINARY):
					IBlob b = AsBlob(ob);
					return b.GetBytes(0, (int)b.Length);
				case (SQLTypes.NULL):
					return ob;
				case (SQLTypes.OTHER):
					return ob;
				case (SQLTypes.OBJECT):
					return ob;
				case (SQLTypes.DISTINCT):
					// (Not supported)
					return ob;
				case (SQLTypes.STRUCT):
					// (Not supported)
					return ob;
				case (SQLTypes.ARRAY):
					// (Not supported)
					return ob;
				case (SQLTypes.BLOB):
					return AsBlob(ob);
				case (SQLTypes.CLOB):
					return AsClob(ob);
				case (SQLTypes.REF):
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
			if (ob is Data.StreamableObject) {
				IClob clob = AsClob(ob);
				long clob_len = clob.Length;
				if (clob_len < 16384L * 65536L) {
					return clob.GetString(1, (int)clob_len);
				}
				throw new DataException("IClob too large to return as a string.");
			} else if (ob is ByteLongObject) {
				throw new InvalidCastException();
			} else {
				return ob.ToString();
			}
		}

		/// <summary>
		/// Executes the given <see cref="SQLQuery"/> object and fill's in at 
		/// most the top 10 entries of the result set.
		/// </summary>
		/// <param name="query"></param>
		/// <returns></returns>
		internal ResultSet[] ExecuteQuery() {
			if (connection == null)
				throw new InvalidOperationException("The connection was not set.");
			if (connection.State != ConnectionState.Open)
				throw new InvalidOperationException("The connection is closed.");

			if (queries == null)
				throw new InvalidOperationException("The command text was not set.");

			// Allocate the result set for this batch
			ResultSet[] results = InternalResultSetList(queries.Length);

			// Reset the result set index
			multi_result_set_index = 0;

			// For each query,
			for (int i = 0; i < queries.Length; ++i) {
				// Prepare the query
				queries[i].Prepare(escape_processing);
				// Make sure the result set is closed
				results[i].CloseCurrentResult();
			}

			// Execute each query
			connection.ExecuteQueries(queries, results);

			// Post processing on the ResultSet objects
			for (int i = 0; i < queries.Length; ++i) {
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
		public int MaxFieldSize {
			get {
				// Are there limitations here?  Strings can be any size...
				return max_field_size;
			}
			set {
				if (value >= 0) {
					max_field_size = value;
				} else {
					throw new DataException("MaxFieldSize negative.");
				}
			}
		}

		//TODO: move to connection string...
		public int MaxRows {
			get { return max_row_count; }
			set {
				if (value >= 0) {
					max_row_count = value;
				} else {
					throw new DataException("MaxRows negative.");
				}
			}
		}

		///<summary>
		///</summary>
		///<param name="enable"></param>
		public void SetEscapeProcessing(bool enable) {
			escape_processing = enable;
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
						Data.StreamableObject s_object =
									   (Data.StreamableObject)streamable_object_list[i];
						connection.RemoveStreamableObject(s_object);
					}
					streamable_object_list = null;
				}
			} catch (DataException) {
			}
		}

		#endregion

		#region Implementation of IDbCommand

		public void Prepare() {
			if (queries == null || parameters.Count == 0)
				return;

			//TODO: we should handle this better: it's nasty that a set
			//      of parameters are set for all the queries...

			for (int i = 0; i < queries.Length; i++) {
				SQLQuery query = queries[i];
				for (int j = 0; j < parameters.Count; j++) {
					DbParameter parameter = parameters[j];
					query.SetVariable(j, CastHelper.CastToSQLType(parameter.Value, parameter.SqlType, parameter.Size, parameter.Scale));
				}
			}
		}

		public void Cancel() {
			if (result_set_list != null) {
				for (int i = 0; i < result_set_list.Length; ++i) {
					connection.DisposeResult(result_set_list[i].ResultId);
				}
			}
		}

		public DbParameter CreateParameter() {
			//TODO: further checks?
			return new DbParameter();
		}

		IDbDataParameter IDbCommand.CreateParameter() {
			return CreateParameter();
		}

		public int ExecuteNonQuery() {
			ResultSet[] resultSet = ExecuteQuery();
			if (resultSet.Length > 1)
				throw new InvalidOperationException();

			return !resultSet[0].IsUpdate ? -1 : resultSet[0].ToInteger();
		}

		IDataReader IDbCommand.ExecuteReader() {
			return ExecuteReader();
		}

		public DbDataReader ExecuteReader() {
			if (reader != null)
				throw new InvalidOperationException("A reader is already opened for this command.");

			connection.StartState(ConnectionState.Fetching);
			ExecuteQuery();
			reader = new DbDataReader(this);
			reader.Closed += new EventHandler(ReaderClosed);
			return reader;
		}

		private void ReaderClosed(object sender, EventArgs e) {
			connection.EndState(ConnectionState.Fetching);
			reader = null;
		}

		IDataReader IDbCommand.ExecuteReader(CommandBehavior behavior) {
			//TODO: support behaviors...
			if (behavior != CommandBehavior.Default)
				throw new ArgumentException("Behavior '" + behavior + "' not supported.");
			return ExecuteReader();
		}

		public object ExecuteScalar() {
			ResultSet[] result = ExecuteQuery();
			if (result.Length > 1)
				throw new InvalidOperationException();

			if (result[0].RowCount > 1)
				throw new DataException();
			if (result[0].ColumnCount > 1)
				throw new DataException();

			Object ob = result[0].GetRawColumn(0);
			if (ob == null)
				return ob;

			if (connection.IsStrictGetValue) {
				// Convert depending on the column type,
				ColumnDescription col_desc = result[0].GetColumn(0);
				SQLTypes sql_type = col_desc.SQLType;

				return ObjectCast(ob, sql_type);

			}
			// We don't support blobs in a scalar.
			if (ob is ByteLongObject ||
				ob is Data.StreamableObject) {
				throw new DataException();
			}
			return ob;
		}

		public DbConnection Connection {
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

		IDbConnection IDbCommand.Connection {
			get { return connection; }
			set {
				if (!(value is DbConnection))
					throw new ArgumentException();

				Connection = (DbConnection) value;
			}
		}

		IDbTransaction IDbCommand.Transaction {
			get { return transaction; }
			set {
				if (!(value is DbTransaction))
					throw new ArgumentException("Trying to set a transaction that is not a '" + typeof(DbTransaction) + "'.");
				Transaction = (DbTransaction) value;
			}
		}

		public DbTransaction Transaction {
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

		public string CommandText {
			get { return commandText; }
			set {
				if (value != null) {
					//TODO: this is a nasty hack to support multiple results: should be
					//      done on server side...
					string[] parts = value.Split(';');
					queries = new SQLQuery[parts.Length];
					for (int i = 0; i < parts.Length; i++) {
						queries[i] = new SQLQuery(parts[i]);
					}
					commandText = value;
				} else {
					queries = null;
					commandText = null;
				}

				parameters.Clear();
			}
		}

		public int CommandTimeout {
			get { return query_timeout; }
			set {
				if (value >= 0)
					throw new ArgumentException();
				query_timeout = value;
			}
		}

		CommandType IDbCommand.CommandType {
			get { return CommandType.Text; }
			set {
				if (value != CommandType.Text)
					throw new ArgumentException();
			}
		}

		IDataParameterCollection IDbCommand.Parameters {
			get { return parameters; }
		}

		public DbParameterCollection Parameters {
			get {
				if (parameters == null)
					parameters = new DbParameterCollection();
				return parameters;
			}
		}

		//TODO: currently not supported...
		UpdateRowSource IDbCommand.UpdatedRowSource {
			get { return UpdateRowSource.None; }
			set {
				if (value != UpdateRowSource.None)
					throw new ArgumentException();
			}
		}

		#endregion
	}
}