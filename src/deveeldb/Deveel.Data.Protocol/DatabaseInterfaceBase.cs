// 
//  Copyright 2010-2011 Deveel
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
using System.IO;

using Deveel.Data.Client;
using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	///<summary>
	/// An abstract implementation of <see cref="DatabaseInterface"/> that 
	/// provides a connection between a single <see cref="DatabaseConnection"/> and 
	/// a <see cref="IDatabaseInterface"/> implementation.
	///</summary>
	/// <remarks>
	/// This receives database _queries from the ADO.NET layer and dispatches the
	/// queries to the database system.  It also manages <see cref="ResultSet"/>
	/// maps for Query results.
	/// <para>
	/// This implementation does not handle authentication (login) / construction 
	/// of the <see cref="DatabaseConnection"/> object, or disposing of the connection.
	/// </para>
	/// <para>
	/// This implementation ignores the <c>AUTO-COMMIT</c> flag when a Query is executed.
	/// To implement <c>AUTO-COMMIT</c>, you should <c>commit</c> after a Query is 
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
		private readonly IDatabaseHandler databaseHandler;

		/// <summary>
		/// The Database object that represents the context of this database interface.
		/// </summary>
		private Database currentDatabase;

		/// <summary>
		/// The mapping that maps from result id number to <see cref="Table"/> object 
		/// that this ADO.NET connection is currently maintaining.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: All <see cref="Table"/> objects are now valid over a database 
		/// shutdown and init.
		/// </remarks>
		private readonly Dictionary<int, ResultSetInfo> resultSetMap;

		/// <summary>
		/// This is incremented every time a result set is added to the map.
		/// </summary>
		/// <remarks>
		/// This way, we always have a unique key on hand.
		/// </remarks>
		private int uniqueResultId;

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
		private DatabaseConnection dbConnection;

		/// <summary>
		/// Mantains a mapping from streamable object id for a particular object 
		/// that is currently being uploaded to the server. 
		/// </summary>
		/// <remarks>
		/// This maps streamable_object_id to blob id reference.
		/// </remarks>
		private readonly Dictionary<long, IRef> blobIdMap;

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
			databaseHandler = handler;
			if (!String.IsNullOrEmpty(databaseName))
				currentDatabase = handler.GetDatabase(databaseName);

			resultSetMap = new Dictionary<int, ResultSetInfo>();
			blobIdMap = new Dictionary<long, IRef>();
			uniqueResultId = 1;
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
			if (currentDatabase == null)
				throw new InvalidOperationException("None database was selected.");
			if (connection.Database != currentDatabase)
				throw new InvalidOperationException("The connection is established to a different database.");

			this.user = user;
			this.dbConnection = connection;
		}

		/// <summary>
		/// Returns the <see cref="Database"/> that is the context of this interface.
		/// </summary>
		protected Database Database {
			get { return currentDatabase; }
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
			get { return dbConnection; }
		}

		/// <summary>
		/// Gets an object that can be used to log debug information.
		/// </summary>
		protected ILogger Logger {
			//TODO: return an empty debug logger if database is null...
			get { return (currentDatabase != null ? currentDatabase.Context.Logger : null); }
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
			result.LockRoot(-1); // -1 because lock_key not implemented

			// Make a new result id
			int resultId;
			// This ensures this block can handle concurrent updates.
			lock (resultSetMap) {
				resultId = ++uniqueResultId;
				// Add the result to the map.
				resultSetMap[resultId] = result;
			}

			return resultId;
		}

		/// <summary>
		/// Gets the result set with the given resultId.
		/// </summary>
		/// <param name="resultId"></param>
		/// <returns></returns>
		private ResultSetInfo GetResultSet(int resultId) {
			lock (resultSetMap) {
				ResultSetInfo resultSetInfo;
				if (resultSetMap.TryGetValue(resultId, out resultSetInfo))
					return resultSetInfo;

				return null;
			}
		}

		/// <summary>
		/// Disposes of the result set with the given resultId.
		/// </summary>
		/// <param name="resultId"></param>
		/// <remarks>
		/// After this has been called, the GC should garbage the table.
		/// </remarks>
		private void DisposeResultSet(int resultId) {
			// Remove this entry.
			ResultSetInfo table;
			lock (resultSetMap) {
				if (resultSetMap.TryGetValue(resultId, out table))
					resultSetMap.Remove(resultId);
			}
			if (table != null) {
				table.Dispose();
			} else {
				Logger.Error(this, "Attempt to dispose invalid 'resultId'.");
			}
		}

		/// <summary>
		/// Clears the contents of the result set map.
		/// </summary>
		/// <remarks>
		/// This removes all resultId ResultSetInfo maps.
		/// </remarks>
		private void ClearResultSetMap() {
			List<int> keys;
			lock (resultSetMap) {
				keys = new List<int>(resultSetMap.Keys);
			}

			foreach (int resultId in keys) {
				DisposeResultSet(resultId);
			}
		}

		/// <summary>
		/// Wraps an <see cref="Exception"/> thrown by the execution of a Query in 
		/// <see cref="DatabaseConnection"/> with an <see cref="DataException"/> and 
		/// puts the appropriate error messages to the debug log.
		/// </summary>
		/// <param name="e"></param>
		/// <param name="query"></param>
		/// <returns></returns>
		protected DataException HandleExecuteThrowable(Exception e, SqlQuery query) {
			if (e is ParseException) {
				Logger.Warning(this, e);

				// Parse exception when parsing the SQL.
				String msg = e.Message;
				msg = msg.Replace("\r", "");
				return new DbDataException(msg, msg, 35, e);
			}
			if (e is TransactionException) {
				TransactionException te = (TransactionException) e;

				// Output Query that was in error to debug log.
				Logger.Info(this, "Transaction error on: " + query);
				Logger.Info(this, e);

				// Denotes a transaction exception.
				return new DbDataException(e.Message, e.Message, 200 + te.Type, e);
			}

			// Output Query that was in error to debug log.
			Logger.Warning(this, "Exception thrown during Query processing on: " + query);
			Logger.Warning(this, e);

			// Error, we need to return exception to client.
			return new DbDataException(e.Message, e.Message, 1, e);
		}

		/// <summary>
		/// Returns a reference implementation object that handles an object that is
		/// either currently being pushed onto the server from the client, or is being
		/// used to reference a large object in an <see cref="SqlQuery"/>.
		/// </summary>
		/// <param name="streamableObjectId"></param>
		/// <param name="type"></param>
		/// <param name="objectLength"></param>
		/// <returns></returns>
		private IRef GetLargeObjectRefFor(long streamableObjectId, ReferenceType type, long objectLength) {
			// Does this mapping already exist?
			IRef reference;
			if (!blobIdMap.TryGetValue(streamableObjectId, out reference)) {
				// Doesn't exist so create a new blob handler.
				reference = dbConnection.CreateNewLargeObject(type, objectLength);
				// Make the blob id mapping
				blobIdMap[streamableObjectId] = reference;
			}

			return reference;
		}

		/// <summary>
		/// Removes the large object reference from the <see cref="Hashtable"/> for 
		/// the given streamable object id from the <see cref="Hashtable"/>.
		/// </summary>
		/// <param name="streamableObjectId"></param>
		/// <remarks>
		/// This allows the <see cref="IRef"/> to finalize if the runtime does not 
		/// maintain any other pointers to it, and therefore clean up the resources 
		/// in the store.
		/// </remarks>
		/// <returns></returns>
		private IRef FlushLargeObjectRefFromCache(long streamableObjectId) {
			try {
				IRef reference;
				if (!blobIdMap.TryGetValue(streamableObjectId, out reference))
					// This basically means the streamable object hasn't been pushed onto the
					// server.
					throw new DataException("Invalid streamable object id in Query.");

				blobIdMap.Remove(streamableObjectId);
				// Mark the blob as complete
				reference.Complete();
				// And return it.
				return reference;
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <summary>
		/// Checks if the interface is disposed, and if it is generates a 
		/// friendly <see cref="DataException"/> informing the user of this.
		/// </summary>
		protected void CheckNotDisposed() {
			if (disposed) {
				throw new ObjectDisposedException("DatabaseInterface",
				                                  "Database interface was disposed (was the connection closed?)");
			}
		}

		// ---------- Implemented from IDatabaseInterface ----------

		/// <inheritdoc/>
		public abstract bool Login(string defaultSchema, string username, string password, DatabaseEventCallback callback);

		/// <inheritdoc/>
		public void PushStreamableObjectPart(ReferenceType type, long objectId, long objectLength, byte[] buf, long offset, int length) {
			CheckNotDisposed();

			try {
				// Create or retrieve the object managing this binary object_id in this
				// connection.
				IRef reference = GetLargeObjectRefFor(objectId, type, objectLength);
				// Push this part of the blob into the object.
				reference.Write(offset, buf, length);
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		public virtual void ChangeDatabase(string name) {
			CheckNotDisposed();

			try {
				Database database = databaseHandler.GetDatabase(name);
				if (database == null)
					throw new InvalidOperationException("Unable to change the database.");

				if (dbConnection != null)
					dbConnection.Close();

				currentDatabase = database;
			} catch (Exception e) {
				Logger.Error(this, e);
				throw new DataException("Unable to change the database: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public virtual IQueryResponse[] ExecuteQuery(SqlQuery query) {
			CheckNotDisposed();

			// Record the Query start time
			DateTime startTime = DateTime.Now;

			// Where Query result eventually resides.
			ResultSetInfo resultSetInfo;
			int resultId = -1;

			// For each StreamableObject in the SQLQuery object, translate it to a
			// IRef object that presumably has been pre-pushed onto the server from
			// the client.
			bool blobsWereFlushed = false;
			object[] vars = query.Variables;
			if (vars != null) {
				for (int i = 0; i < vars.Length; ++i) {
					object ob = vars[i];
					// This is a streamable object, so convert it to a *IRef
					if (ob != null && ob is StreamableObject) {
						StreamableObject sObject = (StreamableObject) ob;

						// Flush the streamable object from the cache
						// Note that this also marks the blob as complete in the blob store.
						IRef reference = FlushLargeObjectRefFromCache(sObject.Identifier);

						// Set the IRef object in the Query.
						vars[i] = reference;

						// There are blobs in this Query that were written to the blob store.
						blobsWereFlushed = true;
					}
				}
			}

			// After the blobs have been flushed, we must tell the connection to
			// flush and synchronize any blobs that have been written to disk.  This
			// is an important (if subtle) step.
			if (blobsWereFlushed)
				dbConnection.FlushBlobStore();

			// Evaluate the sql Query.
			Table[] results = SqlQueryExecutor.Execute(dbConnection, query);
			IQueryResponse[] responses = new IQueryResponse[results.Length];
			int j = 0;

			foreach (Table result in results) {
				try {
					// Put the result in the result cache...  This will Lock this object
					// until it is removed from the result set cache.  Returns an id that
					// uniquely identifies this result set in future communication.
					// NOTE: This locks the roots of the table so that its contents
					//   may not be altered.
					resultSetInfo = new ResultSetInfo(query, result);
					resultId = AddResultSet(resultSetInfo);
				} catch (Exception e) {
					// If resultId set, then dispose the result set.
					if (resultId != -1)
						DisposeResultSet(resultId);

					// Handle the throwable during Query execution
					throw HandleExecuteThrowable(e, query);
				}

				// The time it took the Query to execute.
				TimeSpan taken = DateTime.Now - startTime;

				// Return the Query response
				responses[j]  = new QueryResponse(resultId, resultSetInfo, (int) taken.TotalMilliseconds, "");
				j++;
			}

			return responses;
		}

		/// <inheritdoc/>
		public ResultPart GetResultPart(int resultId, int startRow, int countRows) {
			CheckNotDisposed();

			ResultSetInfo table = GetResultSet(resultId);
			if (table == null)
				throw new DbDataException("'resultId' invalid.", null, 4, (Exception) null);

			int rowEnd = startRow + countRows;

			if (startRow < 0 || startRow >= table.RowCount ||
			    rowEnd > table.RowCount) {
				throw new DbDataException("Result part out of range.", null, 4, (Exception) null);
			}

			try {
				int colCount = table.ColumnCount;
				ResultPart block = new ResultPart(countRows*colCount);
				for (int r = startRow; r < rowEnd; ++r) {
					for (int c = 0; c < colCount; ++c) {
						TObject value = table.GetCellContents(c, r);

						// If this is a IRef, we must assign it a streamable object
						// id that the client can use to access the large object.
						object clientOb;
						if (value.Object is IRef) {
							IRef reference = (IRef) value.Object;
							clientOb = new StreamableObject(reference.Type, reference.RawSize, reference.Id);
						} else {
							clientOb = value.Object;
						}

						block.Add(clientOb);
					}
				}
				return block;
			} catch (Exception e) {
				Logger.Warning(this, e);
				// If an exception was generated while getting the cell contents, then
				// throw an DataException.
				throw new DbDataException("Exception while reading results: " + e.Message, e.Message, 4, e);
			}

		}

		/// <inheritdoc/>
		public void DisposeResult(int resultId) {
			// Check the IDatabaseInterface is not dispoed
			CheckNotDisposed();
			// Dispose the result
			DisposeResultSet(resultId);
		}


		/// <inheritdoc/>
		public byte[] GetStreamableObjectPart(int resultId, long streamableObjectId, long offset, int len) {
			CheckNotDisposed();

			// NOTE: It's important we handle the 'resultId' here and don't just
			//   treat the 'streamable_object_id' as a direct reference into the
			//   blob store.  If we don't authenticate a streamable object against its
			//   originating result, we can't guarantee the user has permission to
			//   access the data.  This would mean a malicious client could access
			//   BLOB data they may not be permitted to look at.
			//   This also protects us from clients that might send a bogus
			//   streamable_object_id and cause unpredictible results.

			ResultSetInfo table = GetResultSet(resultId);
			if (table == null)
				throw new DbDataException("'resultId' invalid.", null, 4, (Exception) null);

			// Get the large object reference that has been cached in the result set.
			IRef reference = table.GetRef(streamableObjectId);
			if (reference == null)
				throw new DbDataException("'streamable_object_id' invalid.", null, 4, (Exception) null);

			// Restrict the server so that a streamable object part can not exceed
			// 512 KB.
			//TODO: make this configurable...
			if (len > 512*1024)
				throw new DbDataException("Request length exceeds 512 KB", null, 4, (Exception) null);

			try {
				// Read the blob part into the byte array.
				byte[] blobPart = new byte[len];
				reference.Read(offset, blobPart, len);

				// And return as a StreamableObjectPart object.
				return blobPart;
			} catch (IOException e) {
				throw new DbDataException("Exception while reading blob: " + e.Message, e.Message, 4, e);
			}
		}

		/// <inheritdoc/>
		public void DisposeStreamableObject(int resultId, long streamableObjectId) {
			CheckNotDisposed();

			// This actually isn't as an important step as I had originally designed
			// for.  To dispose we simply remove the blob reference from the cache in the
			// result.  If this doesn't happen, nothing seriously bad will happen.

			ResultSetInfo table = GetResultSet(resultId);
			if (table == null)
				throw new DbDataException("'resultId' invalid.", null, 4, (Exception) null);

			// Remove this IRef from the table
			table.RemoveRef(streamableObjectId);
		}


		// ---------- Clean up ----------

		public void Dispose() {
			if (!disposed) {
				Dispose(true);
				GC.SuppressFinalize(this);
				disposed = true;
			}
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				// Clear the result set mapping
				ClearResultSetMap();
				user = null;
				dbConnection = null;
			}
		}

	// ---------- Inner classes ----------

		/// <summary>
		/// The response to a Query.
		/// </summary>
		private sealed class QueryResponse : IQueryResponse {
			private readonly int resultId;
			private readonly ResultSetInfo resultSetInfo;
			private readonly int queryTime;
			private readonly string warnings;

			internal QueryResponse(int resultId, ResultSetInfo resultSetInfo, int queryTime, string warnings) {
				this.resultId = resultId;
				this.resultSetInfo = resultSetInfo;
				this.queryTime = queryTime;
				this.warnings = warnings;
			}

		    public int ResultId {
		        get { return resultId; }
		    }

		    public int QueryTimeMillis {
		        get { return queryTime; }
		    }

		    public int RowCount {
		        get { return resultSetInfo.RowCount; }
		    }

		    public int ColumnCount {
		        get { return resultSetInfo.ColumnCount; }
		    }

		    public ColumnDescription GetColumnDescription(int n) {
				return resultSetInfo.Fields[n];
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
		/// <item>The column topology doesn't change (NOTE: issues with <c>ALTER</c> Query)</item>
		/// <item>Root locking prevents modification to rows.</item>
		/// </list>
		/// </para>
		/// </remarks>
		private sealed class ResultSetInfo {
			/// <summary>
			/// The SqlQuery that was executed to produce this result.
			/// </summary>
			private SqlQuery query;

			/// <summary>
			/// The table that is the result.
			/// </summary>
			private Table result;

			/// <summary>
			/// A set of ColumnDescription that describes each column in the ResultSet.
			/// </summary>
			private ColumnDescription[] colDesc;

			/// <summary>
			/// The <see cref="IList{T}"/> that contains the row index into the table 
			/// for each row of the result.
			/// </summary>
			private IList<int> rowIndexMap;

			/// <summary>
			/// Set to true if the result table has a <see cref="SimpleRowEnumerator"/>, therefore 
			/// guarenteeing we do not need to store a row lookup list.
			/// </summary>
			private readonly bool resultIsSimpleEnum;

			/// <summary>
			/// The number of rows in the result.
			/// </summary>
			private readonly int resultRowCount;

			/// <summary>
			/// Incremented when we Lock roots.
			/// </summary>
			private int locked;

			/// <summary>
			/// A <see cref="Hashtable"/> of blob_reference_id values to <see cref="IRef"/> 
			/// objects used to handle and streamable objects in this result.
			/// </summary>
			private readonly Dictionary<long, IRef> streamableBlobMap;


			/// <summary>
			/// Constructs the result set.
			/// </summary>
			/// <param name="query"></param>
			/// <param name="result"></param>
			public ResultSetInfo(SqlQuery query, Table result) {
				this.query = query;
				this.result = result;
				streamableBlobMap = new Dictionary<long, IRef>();

				resultRowCount = result.RowCount;

				// HACK: Read the contents of the first row so that we can pick up
				//   any errors with reading, and also to fix the 'uniquekey' bug
				//   that causes a new transaction to be started if 'uniquekey' is
				//   a column and the value is resolved later.
				IRowEnumerator rowEnum = result.GetRowEnumerator();
				if (rowEnum.MoveNext()) {
					int row_index = rowEnum.RowIndex;
					for (int c = 0; c < result.ColumnCount; ++c) {
						result.GetCell(c, row_index);
					}
				}

				// If simple enum, note it here
				resultIsSimpleEnum = (rowEnum is SimpleRowEnumerator);
				rowEnum = null;

				// Build 'row_index_map' if not a simple enum
				if (!resultIsSimpleEnum) {
					rowIndexMap = new List<int>(result.RowCount);

					IRowEnumerator en = result.GetRowEnumerator();
					while (en.MoveNext()) {
						rowIndexMap.Add(en.RowIndex);
					}
				}

				// This is a safe operation provides we are shared.
				// Copy all the TableField columns from the table to our own
				// ColumnDescription array, naming each column by what is returned from
				// the 'GetResolvedVariable' method.
				int colCount = result.ColumnCount;
				colDesc = new ColumnDescription[colCount];
				for (int i = 0; i < colCount; ++i) {
					VariableName v = result.GetResolvedVariable(i);
					string fieldName;
					if (v.TableName == null) {
						// This means the column is an alias
						fieldName = "@a" + v.Name;
					} else {
						// This means the column is an schema/table/column reference
						fieldName = "@f" + v;
					}
					colDesc[i] = new ColumnDescription(fieldName, result.GetColumnInfo(i));
				}

				locked = 0;
			}

			/// <summary>
			/// Returns a <see cref="IRef"/> that has been cached in this table object 
			/// by its identifier value.
			/// </summary>
			/// <param name="id"></param>
			/// <returns></returns>
			public IRef GetRef(long id) {
				IRef reference;
				if (!streamableBlobMap.TryGetValue(id, out reference))
					return null;

				return reference;
			}

			/// <summary>
			/// Removes a <see cref="IRef"/> that has been cached in this table object 
			/// by its identifier value.
			/// </summary>
			/// <param name="id"></param>
			public void RemoveRef(long id) {
				streamableBlobMap.Remove(id);
			}

			/// <summary>
			/// Disposes this object.
			/// </summary>
			public void Dispose() {
				while (locked > 0) {
					UnlockRoot(-1);
				}
				result = null;
				rowIndexMap = null;
				colDesc = null;
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
			public TObject GetCellContents(int column, int row) {
				if (locked <= 0)
					throw new Exception("Table roots not locked!");

				int realRow = resultIsSimpleEnum ? row : rowIndexMap[row];
				TObject tob = result.GetCell(column, realRow);

				// If this is a large object reference then cache it so a streamable
				// object can reference it via this result.
				if (tob.Object is IRef) {
					IRef reference = (IRef)tob.Object;
					streamableBlobMap[reference.Id] = reference;
				}

				return tob;
			}

			/// <summary>
			/// Returns the column count.
			/// </summary>
			public int ColumnCount {
				get { return result.ColumnCount; }
			}

			/// <summary>
			/// Returns the row count.
			/// </summary>
			public int RowCount {
				get { return resultRowCount; }
			}

			/// <summary>
			/// Returns the ColumnDescription array of all the columns in the result.
			/// </summary>
			public ColumnDescription[] Fields {
				get { return colDesc; }
			}

			/// <summary>
			/// Locks the root of the result set.
			/// </summary>
			/// <param name="key"></param>
			public void LockRoot(int key) {
				result.LockRoot(key);
				++locked;
			}

			/// <summary>
			/// Unlocks the root of the result set.
			/// </summary>
			/// <param name="key"></param>
			private void UnlockRoot(int key) {
				result.UnlockRoot(key);
				--locked;
			}
		}
	}
}