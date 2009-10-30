//  
//  DatabaseInterfaceBase.cs
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

using Deveel.Data.Client;
using Deveel.Data.Collections;
using Deveel.Data.Control;
using Deveel.Data.Sql;

using Deveel.Diagnostics;

namespace Deveel.Data.Server {
	///<summary>
	/// An abstract implementation of <see cref="DatabaseInterface"/> that 
	/// provides a connection between a single <see cref="DatabaseConnection"/> and 
	/// a <see cref="IDatabaseInterface"/> implementation.
	///</summary>
	/// <remarks>
	/// This receives database commands from the ADO.NET layer and dispatches the
	/// queries to the database system.  It also manages <see cref="ResultSet"/>
	/// maps for command results.
	/// <para>
	/// This implementation does not handle authentication (login) / construction 
	/// of the <see cref="DatabaseConnection"/> object, or disposing of the connection.
	/// </para>
	/// <para>
	/// This implementation ignores the <c>AUTO-COMMIT</c> flag when a command is executed.
	/// To implement <c>AUTO-COMMIT</c>, you should <c>commit</c> after a command is 
	/// executed.
	/// </para>
	/// <para>
	/// <b>Synchronization</b>: This interface is <b>not</b> thread-safe.  To make a 
	/// thread-safe implementation use the <see cref="LockingMechanism"/>.
	/// </para>
	/// <para>
	/// See <see cref="DatabaseInterface"/> for a standard server-side implementation 
	/// of this class.
	/// </para>
	/// </remarks>
	public abstract class DatabaseInterfaceBase : IDatabaseInterface {
		/// <summary>
		/// A pointer to the object that handles database within the
		/// underlying system.
		/// </summary>
		private readonly IDatabaseHandler database_handler;

        /// <summary>
        /// The Database object that represents the context of this database interface.
        /// </summary>
		private Database database;

        /// <summary>
        /// The mapping that maps from result id number to <see cref="Table"/> object 
        /// that this ADO.NET connection is currently maintaining.
        /// </summary>
        /// <remarks>
        /// <b>Note</b>: All <see cref="Table"/> objects are now valid over a database 
        /// shutdown and init.
        /// </remarks>
		private readonly Hashtable result_set_map;

        /// <summary>
        /// This is incremented every time a result set is added to the map.
        /// </summary>
        /// <remarks>
        /// This way, we always have a unique key on hand.
        /// </remarks>
		private int unique_result_id;

        /// <summary>
        /// Access to information regarding the user logged in on this connection.
        /// </summary>
        /// <remarks>
        /// If no user is logged in, this is left as 'null'.  We can also use this to
        /// retreive the <see cref="Database"/> object the user is logged into.
        /// </remarks>
		private User user;

        /// <summary>
        /// The database connection transaction.
        /// </summary>
		private DatabaseConnection database_connection;

        /// <summary>
        /// The SQL parser object for this interface.
        /// </summary>
        /// <remarks>
        /// When a statement is being parsed, this object is sychronized.
        /// </remarks>
		private SqlCommandExecutor sql_executor;
		//  private SQL sql_parser;

        /// <summary>
        /// Mantains a mapping from streamable object id for a particular object 
        /// that is currently being uploaded to the server. 
        /// </summary>
        /// <remarks>
        /// This maps streamable_object_id to blob id reference.
        /// </remarks>
		private Hashtable blob_id_map;

        /// <summary>
        /// Set to true when this database interface is disposed.
        /// </summary>
		private bool disposed;


		///<summary>
        /// Sets up the database interface.
		///</summary>
		/// <param name="handler"></param>
		/// <param name="databaseName"></param>
		protected DatabaseInterfaceBase(IDatabaseHandler handler, string databaseName) {
			database_handler = handler;
			if (databaseName != null && databaseName.Length > 0)
				database = handler.GetDatabase(databaseName);

			result_set_map = new Hashtable();
			blob_id_map = new Hashtable();
			unique_result_id = 1;
			disposed = false;
		}

		~DatabaseInterfaceBase() {
			Dispose(false);
		}

		// ---------- Utility methods ----------

        /// <summary>
        /// Initializes this database interface with a <see cref="User"/> and 
        /// <see cref="DatabaseConnection"/> object.
        /// </summary>
        /// <param name="user"></param>
        /// <param name="connection"></param>
        /// <remarks>
        /// This would typically be called from inside an authentication method, or 
        /// from <see cref="Login"/>.  This must be set before the object can be used.
        /// </remarks>
		protected void Init(User user, DatabaseConnection connection) {
			if (database == null)
				throw new InvalidOperationException("None database was selected.");
			if (connection.Database != database)
				throw new InvalidOperationException("The connection is established to a different database.");

			this.user = user;
			this.database_connection = connection;
			// Set up the sql parser.
			sql_executor = new SqlCommandExecutor();
			//    sql_parser = new SQL(new StringReader(""));
		}

        /// <summary>
        /// Returns the <see cref="Database"/> that is the context of this interface.
        /// </summary>
	    protected Database Database {
	        get { return database; }
	    }

        /// <summary>
        /// Returns the <see cref="User"/> object for this connection.
        /// </summary>
	    protected User User {
	        get { return user; }
	    }

        /// <summary>
        /// Returns the <see cref="DatabaseConnection"/> objcet for this connection.
        /// </summary>
	    protected DatabaseConnection DatabaseConnection {
	        get { return database_connection; }
	    }

        /// <summary>
        /// Adds this result set to the list of result sets being handled 
        /// through this processor.
        /// </summary>
        /// <param name="result"></param>
        /// <returns>
        /// Returns a number that unique identifies the result set.
        /// </returns>
		private int AddResultSet(ResultSetInfo result) {
			// Lock the roots of the result set.
			result.LockRoot(-1);  // -1 because lock_key not implemented

			// Make a new result id
			int result_id;
			// This ensures this block can handle concurrent updates.
			lock (result_set_map) {
				result_id = ++unique_result_id;
				// Add the result to the map.
				result_set_map[result_id] = result;
			}

			return result_id;
		}

        /// <summary>
        /// Gets the result set with the given result_id.
        /// </summary>
        /// <param name="result_id"></param>
        /// <returns></returns>
		private ResultSetInfo GetResultSet(int result_id) {
			lock (result_set_map) {
				return (ResultSetInfo)result_set_map[result_id];
			}
		}

        /// <summary>
        /// Disposes of the result set with the given result_id.
        /// </summary>
        /// <param name="result_id"></param>
        /// <remarks>
        /// After this has been called, the GC should garbage the table.
        /// </remarks>
		private void DisposeResultSet(int result_id) {
			// Remove this entry.
			ResultSetInfo table;
			lock (result_set_map) {
				table = (ResultSetInfo) result_set_map[result_id];
				result_set_map.Remove(result_id);
			}
			if (table != null) {
				table.Dispose();
			} else {
				Debug.Write(DebugLevel.Error, this,
							"Attempt to dispose invalid 'result_id'.");
			}
		}

		/// <summary>
		/// Clears the contents of the result set map.
		/// </summary>
		/// <remarks>
		/// This removes all result_id ResultSetInfo maps.
		/// </remarks>
		protected void ClearResultSetMap() {
			IEnumerator keys;
			ArrayList list;
			lock (result_set_map) {
				keys = result_set_map.Keys.GetEnumerator();

				list = new ArrayList();
				while (keys.MoveNext()) {
					list.Add(keys.Current);
				}
			}
			keys = list.GetEnumerator();

			while (keys.MoveNext()) {
				int result_id = (int)keys.Current;
				DisposeResultSet(result_id);
			}
		}

		/// <summary>
		/// Wraps an <see cref="Exception"/> thrown by the execution of a command in 
		/// <see cref="DatabaseConnection"/> with an <see cref="DataException"/> and 
		/// puts the appropriate error messages to the debug log.
		/// </summary>
		/// <param name="e"></param>
		/// <param name="command"></param>
		/// <returns></returns>
		protected DataException HandleExecuteThrowable(Exception e, SqlCommand command) {
			if (e is ParseException) {
				Debug.WriteException(DebugLevel.Warning, e);

				// Parse exception when parsing the SQL.
				String msg = e.Message;
				msg = msg.Replace("\r", "");
				return new DbDataException(msg, msg, 35, e);
			}
			if (e is TransactionException) {
				TransactionException te = (TransactionException)e;

				// Output command that was in error to debug log.
				Debug.Write(DebugLevel.Information, this, "Transaction error on: " + command);
				Debug.WriteException(DebugLevel.Information, e);

				// Denotes a transaction exception.
				return new DbDataException(e.Message, e.Message, 200 + te.Type, e);
			} else {

				// Output command that was in error to debug log.
				Debug.Write(DebugLevel.Warning, this,
							"Exception thrown during command processing on: " + command);
				Debug.WriteException(DebugLevel.Warning, e);

				// Error, we need to return exception to client.
				return new DbDataException(e.Message, e.Message, 1, e);

			}

		}

		/// <summary>
		/// Returns a reference implementation object that handles an object that is
		/// either currently being pushed onto the server from the client, or is being
		/// used to reference a large object in an <see cref="SQLQuery"/>.
		/// </summary>
		/// <param name="streamable_object_id"></param>
		/// <param name="type"></param>
		/// <param name="object_length"></param>
		/// <returns></returns>
		private IRef GetLargeObjectRefFor(long streamable_object_id, ReferenceType type, long object_length) {
			// Does this mapping already exist?
			long s_ob_id = streamable_object_id;
			Object ob = blob_id_map[s_ob_id];
			if (ob == null) {
				// Doesn't exist so create a new blob handler.
				IRef reference = database_connection.CreateNewLargeObject(type, object_length);
				// Make the blob id mapping
				blob_id_map[s_ob_id] = reference;
				// And return it
				return reference;
			} else {
				// Exists so use this blob reference.
				return (IRef)ob;
			}
		}

		/// <summary>
		/// Returns a reference object that handles the given streamable object 
		/// id in this database interface.
		/// </summary>
		/// <param name="streamable_object_id"></param>
		/// <remarks>
		/// Unlike the other <see cref="GetLargeObjectRefFor(long,byte,long)"/> method, 
		/// this will not create a new handle if it has not already been formed before 
		/// by this connection.  If the large object reference is not found an exception 
		/// is generated.
		/// </remarks>
		/// <returns></returns>
		private IRef GetLargeObjectRefFor(long streamable_object_id) {
			long s_ob_id = streamable_object_id;
			Object ob = blob_id_map[s_ob_id];
			if (ob == null) {
				// This basically means the streamable object hasn't been pushed onto the
				// server.
				throw new DataException("Invalid streamable object id in command.");
			} else {
				return (IRef)ob;
			}
		}

		/// <summary>
		/// Removes the large object reference from the <see cref="Hashtable"/> for 
		/// the given streamable object id from the <see cref="Hashtable"/>.
		/// </summary>
		/// <param name="streamable_object_id"></param>
		/// <remarks>
		/// This allows the <see cref="IRef"/> to finalize if the runtime does not 
		/// maintain any other pointers to it, and therefore clean up the resources 
		/// in the store.
		/// </remarks>
		/// <returns></returns>
		private IRef FlushLargeObjectRefFromCache(long streamable_object_id) {
			try {
				long s_ob_id = streamable_object_id;
				if (!blob_id_map.ContainsKey(s_ob_id)) {
					// This basically means the streamable object hasn't been pushed onto the
					// server.
					throw new DataException("Invalid streamable object id in command.");
				} else {
					Object ob = blob_id_map[s_ob_id];
					blob_id_map.Remove(s_ob_id);
					IRef reference = (IRef)ob;
					// Mark the blob as complete
					reference.Complete();
					// And return it.
					return reference;
				}
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <summary>
		/// Disposes all resources associated with this object.
		/// </summary>
		/// <remarks>
		/// This clears the <see cref="ResultSet"/> map, and nulls all references to 
		/// help the garbage collector. This method would normally be called from 
		/// implementations of the <see cref="Dispose()"/>.
		/// </remarks>
		protected void InternalDispose() {
			disposed = true;
			// Clear the result set mapping
			ClearResultSetMap();
			user = null;
			database_connection = null;
			sql_executor = null;
		}

		/// <summary>
		/// Checks if the interface is disposed, and if it is generates a 
		/// friendly <see cref="DataException"/> informing the user of this.
		/// </summary>
		protected void CheckNotDisposed() {
			if (disposed) {
				throw new DataException("Database interface was disposed (was the connection closed?)");
			}
		}

		// ---------- Implemented from IDatabaseInterface ----------

		/// <inheritdoc/>
		public abstract bool Login(string default_schema, string username, string password, IDatabaseCallBack call_back);

		/// <inheritdoc/>
		public void PushStreamableObjectPart(ReferenceType type, long object_id, long object_length, byte[] buf, long offset, int length) {
			CheckNotDisposed();

			try {
				// Create or retrieve the object managing this binary object_id in this
				// connection.
				IRef reference = GetLargeObjectRefFor(object_id, type, object_length);
				// Push this part of the blob into the object.
				reference.Write(offset, buf, length);
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		public virtual void ChangeDatabase(string name) {
			CheckNotDisposed();

			try {
				Database db = database_handler.GetDatabase(name);
				if (db == null)
					throw new InvalidOperationException("Unable to change the database.");
				if (database_connection != null)
					database_connection.Close();

				database = db;
			} catch(Exception e) {
				Debug.WriteException(e);
				throw new DataException("Unable to change the database: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public virtual IQueryResponse ExecuteQuery(SqlCommand command) {
			CheckNotDisposed();

			// Record the command start time
			DateTime start_time = DateTime.Now;
			// Where command result eventually resides.
			ResultSetInfo result_set_info;
			int result_id = -1;

			// For each StreamableObject in the SQLQuery object, translate it to a
			// IRef object that presumably has been pre-pushed onto the server from
			// the client.
			bool blobs_were_flushed = false;
			Object[] vars = command.Variables;
			if (vars != null) {
				for (int i = 0; i < vars.Length; ++i) {
					Object ob = vars[i];
					// This is a streamable object, so convert it to a *IRef
					if (ob != null && ob is StreamableObject) {
						StreamableObject s_object = (StreamableObject)ob;
						// Flush the streamable object from the cache
						// Note that this also marks the blob as complete in the blob store.
						IRef reference = FlushLargeObjectRefFromCache(s_object.Identifier);
						// Set the IRef object in the command.
						vars[i] = reference;
						// There are blobs in this command that were written to the blob store.
						blobs_were_flushed = true;
					}
				}
			}

			// After the blobs have been flushed, we must tell the connection to
			// flush and synchronize any blobs that have been written to disk.  This
			// is an important (if subtle) step.
			if (blobs_were_flushed) {
				database_connection.FlushBlobStore();
			}

			try {

				// Evaluate the sql command.
				Table result = sql_executor.Execute(database_connection, command);

				// Put the result in the result cache...  This will Lock this object
				// until it is removed from the result set cache.  Returns an id that
				// uniquely identifies this result set in future communication.
				// NOTE: This locks the roots of the table so that its contents
				//   may not be altered.
				result_set_info = new ResultSetInfo(command, result);
				result_id = AddResultSet(result_set_info);

			} catch (Exception e) {
				// If result_id set, then dispose the result set.
				if (result_id != -1) {
					DisposeResultSet(result_id);
				}

				// Handle the throwable during command execution
				throw HandleExecuteThrowable(e, command);

			}

			// The time it took the command to execute.
			TimeSpan taken = DateTime.Now - start_time;

			// Return the command response
			return new QueryResponse(result_id, result_set_info, (int)taken.TotalMilliseconds, "");

		}

		/// <inheritdoc/>
		public ResultPart GetResultPart(int result_id, int row_number, int row_count) {
			CheckNotDisposed();

			ResultSetInfo table = GetResultSet(result_id);
			if (table == null) {
				throw new DbDataException("'result_id' invalid.", null, 4,
										(Exception)null);
			}

			int row_end = row_number + row_count;

			if (row_number < 0 || row_number >= table.RowCount ||
				row_end > table.RowCount) {
				throw new DbDataException("Result part out of range.", null, 4, (Exception) null);
			}

			try {
				int col_count = table.ColumnCount;
				ResultPart block = new ResultPart(row_count * col_count);
				for (int r = row_number; r < row_end; ++r) {
					for (int c = 0; c < col_count; ++c) {
						TObject t_object = table.GetCellContents(c, r);
						// If this is a IRef, we must assign it a streamable object
						// id that the client can use to access the large object.
						Object client_ob;
						if (t_object.Object is IRef) {
							IRef reference = (IRef)t_object.Object;
							client_ob = new StreamableObject(reference.Type, reference.RawSize, reference.Id);
						} else {
							client_ob = t_object.Object;
						}
						block.Add(client_ob);
					}
				}
				return block;
			} catch (Exception e) {
				Debug.WriteException(DebugLevel.Warning, e);
				// If an exception was generated while getting the cell contents, then
				// throw an DataException.
				throw new DbDataException(
					"Exception while reading results: " + e.Message,
					e.Message, 4, e);
			}

		}

		/// <inheritdoc/>
		public void DisposeResult(int result_id) {
			// Check the IDatabaseInterface is not dispoed
			CheckNotDisposed();
			// Dispose the result
			DisposeResultSet(result_id);
		}


		/// <inheritdoc/>
		public StreamableObjectPart GetStreamableObjectPart(int result_id, long streamable_object_id, long offset, int len) {
			CheckNotDisposed();

			// NOTE: It's important we handle the 'result_id' here and don't just
			//   treat the 'streamable_object_id' as a direct reference into the
			//   blob store.  If we don't authenticate a streamable object against its
			//   originating result, we can't guarantee the user has permission to
			//   access the data.  This would mean a malicious client could access
			//   BLOB data they may not be permitted to look at.
			//   This also protects us from clients that might send a bogus
			//   streamable_object_id and cause unpredictible results.

			ResultSetInfo table = GetResultSet(result_id);
			if (table == null) {
				throw new DbDataException("'result_id' invalid.", null, 4,
										(Exception)null);
			}

			// Get the large object reference that has been cached in the result set.
			IRef reference = table.GetRef(streamable_object_id);
			if (reference == null) {
				throw new DbDataException("'streamable_object_id' invalid.", null, 4,
										(Exception)null);
			}

			// Restrict the server so that a streamable object part can not exceed
			// 512 KB.
			if (len > 512 * 1024) {
				throw new DbDataException("Request length exceeds 512 KB", null, 4,
										(Exception)null);
			}

			try {
				// Read the blob part into the byte array.
				byte[] blob_part = new byte[len];
				reference.Read(offset, blob_part, len);

				// And return as a StreamableObjectPart object.
				return new StreamableObjectPart(blob_part);

			} catch (IOException e) {
				throw new DbDataException(
					"Exception while reading blob: " + e.Message,
					e.Message, 4, e);
			}

		}

		/// <inheritdoc/>
		public void DisposeStreamableObject(int result_id, long streamable_object_id) {
			CheckNotDisposed();

			// This actually isn't as an important step as I had originally designed
			// for.  To dispose we simply remove the blob reference from the cache in the
			// result.  If this doesn't happen, nothing seriously bad will happen.

			ResultSetInfo table = GetResultSet(result_id);
			if (table == null) {
				throw new DbDataException("'result_id' invalid.", null, 4,
										(Exception)null);
			}

			// Remove this IRef from the table
			table.RemoveRef(streamable_object_id);

		}


		// ---------- Clean up ----------

		void IDisposable.Dispose() {
			Dispose(true);
		}

		protected abstract void Dispose();

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					if (!disposed) {
						GC.SuppressFinalize(this);
						Dispose();
					}
				} catch(Exception) {
					
				}
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// The response to a command.
		/// </summary>
		private sealed class QueryResponse : IQueryResponse {
			private readonly int result_id;
			private readonly ResultSetInfo result_set_info;
			private readonly int query_time;
			private readonly String warnings;

			internal QueryResponse(int result_id, ResultSetInfo result_set_info,
							 int query_time, String warnings) {
				this.result_id = result_id;
				this.result_set_info = result_set_info;
				this.query_time = query_time;
				this.warnings = warnings;
			}

		    public int ResultId {
		        get { return result_id; }
		    }

		    public int QueryTimeMillis {
		        get { return query_time; }
		    }

		    public int RowCount {
		        get { return result_set_info.RowCount; }
		    }

		    public int ColumnCount {
		        get { return result_set_info.ColumnCount; }
		    }

		    public ColumnDescription GetColumnDescription(int n) {
				return result_set_info.Fields[n];
			}

		    public string Warnings {
		        get { return warnings; }
		    }
		}

		/// <summary>
		/// Whenever a <see cref="ResultSet"/> is generated, this object contains 
		/// the result set.
		/// </summary>
		/// <remarks>
		/// This class only allows calls to safe methods in Table.
		/// <para>
		/// <b>Note</b>: This is safe provided,
		/// <list type="number">
		/// <item>The column topology doesn't change (NOTE: issues with <c>ALTER</c> command)</item>
		/// <item>Root locking prevents modification to rows.</item>
		/// </list>
		/// </para>
		/// </remarks>
		private sealed class ResultSetInfo {
			/// <summary>
			/// The SqlCommand that was executed to produce this result.
			/// </summary>
			private SqlCommand command;

			/// <summary>
			/// The table that is the result.
			/// </summary>
			private Table result;

			/// <summary>
			/// A set of ColumnDescription that describes each column in the ResultSet.
			/// </summary>
			private ColumnDescription[] col_desc;

			/// <summary>
			/// The <see cref="IntegerVector"/> that contains the row index into the table 
			/// for each row of the result.
			/// </summary>
			private IntegerVector row_index_map;

			/// <summary>
			/// Set to true if the result table has a <see cref="SimpleRowEnumerator"/>, therefore 
			/// guarenteeing we do not need to store a row lookup list.
			/// </summary>
			private readonly bool result_is_simple_enum;

			/// <summary>
			/// The number of rows in the result.
			/// </summary>
			private readonly int result_row_count;

			/// <summary>
			/// Incremented when we Lock roots.
			/// </summary>
			private int locked;

			/// <summary>
			/// A <see cref="Hashtable"/> of blob_reference_id values to <see cref="IRef"/> 
			/// objects used to handle and streamable objects in this result.
			/// </summary>
			private readonly Hashtable streamable_blob_map;


			/// <summary>
			/// Constructs the result set.
			/// </summary>
			/// <param name="command"></param>
			/// <param name="table"></param>
			internal ResultSetInfo(SqlCommand command, Table table) {
				this.command = command;
				this.result = table;
				this.streamable_blob_map = new Hashtable();

				result_row_count = table.RowCount;

				// HACK: Read the contents of the first row so that we can pick up
				//   any errors with reading, and also to fix the 'uniquekey' bug
				//   that causes a new transaction to be started if 'uniquekey' is
				//   a column and the value is resolved later.
				IRowEnumerator row_enum = table.GetRowEnumerator();
				if (row_enum.MoveNext()) {
					int row_index = row_enum.RowIndex;
					for (int c = 0; c < table.ColumnCount; ++c) {
						table.GetCellContents(c, row_index);
					}
				}
				// If simple enum, note it here
				result_is_simple_enum = (row_enum is SimpleRowEnumerator);
				row_enum = null;

				// Build 'row_index_map' if not a simple enum
				if (!result_is_simple_enum) {
					row_index_map = new IntegerVector(table.RowCount);
					IRowEnumerator en = table.GetRowEnumerator();
					while (en.MoveNext()) {
						row_index_map.AddInt(en.RowIndex);
					}
				}

				// This is a safe operation provides we are shared.
				// Copy all the TableField columns from the table to our own
				// ColumnDescription array, naming each column by what is returned from
				// the 'GetResolvedVariable' method.
				int col_count = table.ColumnCount;
				col_desc = new ColumnDescription[col_count];
				for (int i = 0; i < col_count; ++i) {
					Variable v = table.GetResolvedVariable(i);
					String field_name;
					if (v.TableName == null) {
						// This means the column is an alias
						field_name = "@a" + v.Name;
					} else {
						// This means the column is an schema/table/column reference
						field_name = "@f" + v.ToString();
					}
					col_desc[i] =
							   table.GetColumnDef(i).ColumnDescriptionValue(field_name);
					//        col_desc[i] = new ColumnDescription(field_name, table.getFieldAt(i));
				}

				locked = 0;
			}

			/// <summary>
			/// Returns the SqlCommand that was used to produce this result.
			/// </summary>
			private SqlCommand SqlCommand {
				get { return command; }
			}

			/// <summary>
			/// Returns a <see cref="IRef"/> that has been cached in this table object 
			/// by its identifier value.
			/// </summary>
			/// <param name="id"></param>
			/// <returns></returns>
			internal IRef GetRef(long id) {
				return (IRef)streamable_blob_map[id];
			}

			/// <summary>
			/// Removes a <see cref="IRef"/> that has been cached in this table object 
			/// by its identifier value.
			/// </summary>
			/// <param name="id"></param>
			internal void RemoveRef(long id) {
				streamable_blob_map.Remove(id);
			}

			/// <summary>
			/// Disposes this object.
			/// </summary>
			internal void Dispose() {
				while (locked > 0) {
					UnlockRoot(-1);
				}
				result = null;
				row_index_map = null;
				col_desc = null;
			}

			/// <summary>
			/// Gets the cell contents of the cell at the given row/column.
			/// </summary>
			/// <param name="column"></param>
			/// <param name="row"></param>
			/// <remarks>
			/// Safe only if roots are locked.
			/// </remarks>
			/// <returns></returns>
			internal TObject GetCellContents(int column, int row) {
				if (locked > 0) {
					int real_row;
					real_row = result_is_simple_enum ? row : row_index_map[row];
					TObject tob = result.GetCellContents(column, real_row);

					// If this is a large object reference then cache it so a streamable
					// object can reference it via this result.
					if (tob.Object is IRef) {
						IRef reference = (IRef)tob.Object;
						streamable_blob_map[reference.Id] = reference;
					}

					return tob;
				} else {
					throw new Exception("Table roots not locked!");
				}
			}

			/// <summary>
			/// Returns the column count.
			/// </summary>
			internal int ColumnCount {
				get { return result.ColumnCount; }
			}

			/// <summary>
			/// Returns the row count.
			/// </summary>
			internal int RowCount {
				get { return result_row_count; }
			}

			/// <summary>
			/// Returns the ColumnDescription array of all the columns in the result.
			/// </summary>
			internal ColumnDescription[] Fields {
				get { return col_desc; }
			}

			/// <summary>
			/// Locks the root of the result set.
			/// </summary>
			/// <param name="key"></param>
			internal void LockRoot(int key) {
				result.LockRoot(key);
				++locked;
			}

			/// <summary>
			/// Unlocks the root of the result set.
			/// </summary>
			/// <param name="key"></param>
			void UnlockRoot(int key) {
				result.UnlockRoot(key);
				--locked;
			}
		}
	}
}