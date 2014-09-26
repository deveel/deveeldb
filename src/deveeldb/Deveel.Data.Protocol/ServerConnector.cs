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
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	public abstract class ServerConnector : IConnector {
		private readonly Dictionary<long, IRef> blobIdMap;

		protected ServerConnector(IDatabase database) {
			if (database == null) 
				throw new ArgumentNullException("database");

			Database = database;
			resultMap = new Dictionary<int, QueryResult>();
			blobIdMap = new Dictionary<long, IRef>();
			uniqueResultId = 1;
		}

		public AuthenticatedSession Session { get; protected set; }

		public ConnectorState CurrentState { get; private set; }

		protected ILogger Logger {
			get { return Database.Context.Logger; }
		}

		protected IDatabase Database { get; private set; }

		private void AssertNotDisposed() {
			if (CurrentState == ConnectorState.Disposed)
				throw new ObjectDisposedException(GetType().AssemblyQualifiedName);
		}

		protected void ChangeState(ConnectorState newState) {
			AssertNotDisposed();
			CurrentState = newState;
		}

		protected void OpenConnector() {
			try {
				OnConnectorOpen();
				ChangeState(ConnectorState.Processing);
			} catch (Exception ex) {
				Logger.Error(this, "Error when opening the connector.");
				Logger.Error(this, ex);
			}
		}

		protected virtual void OnConnectorOpen() {
		}

		protected void CloseConnector() {
			try {
				OnCloseConnector();
				ChangeState(ConnectorState.Closed);
			} catch (Exception ex) {
				Logger.Error(this, "Error when closing the connector.");
				Logger.Error(this, ex);				
			}
		}

		protected virtual void OnCloseConnector() {
			
		}

		protected virtual EncryptionData GetEncryptionData() {
			return null;
		}

		protected virtual bool Authenticate(string defaultSchema, string username, string password) {
			if (CurrentState == ConnectorState.Authenticated &&
			    Session != null)
				throw new InvalidOperationException("Already authenticated.");

			if (Logger.IsInterestedIn(Diagnostics.LogLevel.Debug)) {
				// Output the instruction to the _queries log.
				Logger.DebugFormat(this, "[CLIENT] [{0}] - Log in", username);
			}

			if (Logger.IsInterestedIn(LogLevel.Info)) {
				Logger.InfoFormat(this, "Authenticate User: {0}", username);
			}

			var user = Database.AuthenticateUser(username, password, null);

			if (user == null) 
				return false;

			IDatabaseConnection connection = Database.CreateNewConnection(user, null);

			// Put the connection in exclusive mode
			LockingMechanism locker = connection.LockingMechanism;
			locker.SetMode(LockingMode.Exclusive);

			try {
				// By default, connections are auto-commit
				connection.AutoCommit = true;

				// Set the default schema for this connection if it exists
				if (connection.SchemaExists(defaultSchema)) {
					connection.SetDefaultSchema(defaultSchema);
				} else {
					Logger.WarningFormat(this, "Couldn't change to '{0}' schema.", defaultSchema);

					// If we can't change to the schema then change to the APP schema
					connection.SetDefaultSchema(ConfigDefaultValues.DefaultSchema);
				}
			} finally {
				try {
					connection.Commit();
				} catch (TransactionException e) {
					// Just issue a warning...
					Logger.Warning(this, e);
				} finally {
					// Guarentee that we unluck from EXCLUSIVE
					locker.FinishMode(LockingMode.Exclusive);
				}
			}

			Session = new AuthenticatedSession(user, connection);
			return true;
		}

		private IRef FlushLargeObjectRefFromCache(long objectId) {
			try {
				IRef reference;
				if (!blobIdMap.TryGetValue(objectId, out reference))
					// This basically means the streamable object hasn't been pushed onto the
					// server.
					throw new DatabaseException("Invalid streamable object id in Query.");

				blobIdMap.Remove(objectId);

				// Mark the blob as complete
				reference.Complete();

				// And return it.
				return reference;
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new DatabaseException("IO Error: " + e.Message, e);
			}
		}

		protected long CreateStreamableObject(ReferenceType referenceType, long length) {
			lock (blobIdMap) {
				try {
					var obj = Session.Connection.CreateLargeObject(referenceType, length);
					blobIdMap[obj.Id] = obj;
					return obj.Id;
				} catch (Exception ex) {
					Logger.ErrorFormat(this, "A request to create an object of type {0} with length {1} caused and error.", referenceType, length);
					Logger.Error(this, ex);
					throw;
				}
			}
		}

		protected IRef GetStreamableObject(long objectId) {
			// TODO: Access the blob store directly if cannot find it in cache ...
			lock (blobIdMap) {
				IRef obj;
				if (!blobIdMap.TryGetValue(objectId, out obj))
					return null;

				return obj;
			}
		}

		protected bool DisposeStreamableObject(long objectId) {
			lock (blobIdMap) {
				IRef obj;
				if (!blobIdMap.TryGetValue(objectId, out obj))
					return false;

				if (obj is IDisposable)
					(obj as IDisposable).Dispose();

				return blobIdMap.Remove(objectId);
			}
		}

		private IQueryResponse[] DoExecuteQuery(string text, IEnumerable<object> parameters) {
			// Record the Query start time
			DateTime startTime = DateTime.Now;

			// Where Query result eventually resides.
			int resultId = -1;

			// For each StreamableObject in the query object, translate it to a
			// IRef object that presumably has been pre-pushed onto the server from
			// the client.
			var vars = new List<object>();
			if (parameters != null) {
				foreach (var parameter in parameters) {
					var preparedParam = parameter;
					if (preparedParam is StreamableObject) {
						var obj = (StreamableObject) preparedParam;

						// Flush the streamable object from the cache
						// Note that this also marks the blob as complete in the blob store.
						IRef reference = FlushLargeObjectRefFromCache(obj.Identifier);

						// Set the IRef object in the Query.
						preparedParam = reference;
					}

					vars.Add(preparedParam);
				}
			}

			// Evaluate the sql Query.
			var query = new SqlQuery(text);
			foreach (var param in vars) {
				query.AddVariable(param);
			}

			Table[] results = SqlQueryExecutor.Execute(Session.Connection, query);
			var responses = new IQueryResponse[results.Length];
			int j = 0;

			foreach (Table result in results) {
				QueryResult queryResult;
				try {
					// Put the result in the result cache...  This will Lock this object
					// until it is removed from the result set cache.  Returns an id that
					// uniquely identifies this result set in future communication.
					// NOTE: This locks the roots of the table so that its contents
					//   may not be altered.
					queryResult = new QueryResult(query, result);
					resultId = AddResult(queryResult);
				} catch (Exception e) {
					// If resultId set, then dispose the result set.
					if (resultId != -1)
						DisposeResult(resultId);

					throw;
				}

				// The time it took the Query to execute.
				TimeSpan taken = DateTime.Now - startTime;

				// Return the Query response
				responses[j]  = new QueryResponse(resultId, queryResult, (int) taken.TotalMilliseconds, "");
				j++;
			}

			return responses;
		}

		private readonly Dictionary<int, QueryResult> resultMap;

		private int uniqueResultId;

		private int AddResult(QueryResult result) {
			// Lock the roots of the result set.
			result.LockRoot(-1); // -1 because lock_key not implemented

			// Make a new result id
			int resultId;
			// This ensures this block can handle concurrent updates.
			lock (resultMap) {
				resultId = ++uniqueResultId;
				// Add the result to the map.
				resultMap[resultId] = result;
			}

			return resultId;
		}

		private QueryResult GetResult(int resultId) {
			lock (resultMap) {
				QueryResult result;
				return resultMap.TryGetValue(resultId, out result) ? result : null;
			}
		}

		protected QueryResultPart GetResultPart(int resultId, int startRow, int countRows) {
			AssertNotDisposed();

			QueryResult table = GetResult(resultId);
			if (table == null)
				throw new DatabaseException("'resultId' invalid.");

			int rowEnd = startRow + countRows;

			if (startRow < 0 || startRow >= table.RowCount ||
			    rowEnd > table.RowCount) {
				throw new DatabaseException("Result part out of range.");
			}

			try {
				int colCount = table.ColumnCount;
				var block = new QueryResultPart(colCount);
				for (int r = startRow; r < rowEnd; ++r) {
					var row = new object[colCount];
					for (int c = 0; c < colCount; ++c) {
						TObject value = table.GetCellContents(c, r);

						// If this is a IRef, we must assign it a streamable object
						// id that the client can use to access the large object.
						object clientOb;
						if (value.Object is IRef) {
							var reference = (IRef) value.Object;
							clientOb = new StreamableObject(reference.Type, reference.RawSize, reference.Id);
						} else {
							clientOb = value.Object;
						}

						row[c] = clientOb;
					}

					block.AddRow(row);
				}
				return block;
			} catch (Exception e) {
				Logger.Warning(this, e);
				// If an exception was generated while getting the cell contents, then
				// throw an DataException.
				throw new DatabaseException("Exception while reading results: " + e.Message, e);
			}
		}

		protected void DisposeResult(int resultId) {
			// Remove this entry.
			QueryResult result;
			lock (resultMap) {
				if (resultMap.TryGetValue(resultId, out result))
					resultMap.Remove(resultId);
			}
			if (result != null) {
				result.Dispose();
			} else {
				Logger.Error(this, "Attempt to dispose invalid 'resultId'.");
			}
		}

		protected IQueryResponse[] ExecuteQuery(string text, IEnumerable<object> parameters) {
			// Log this Query if Query logging is enabled
			if (Logger.IsInterestedIn(LogLevel.Debug)) {
				// Output the instruction to the _queries log.
				Logger.DebugFormat(this, "[CLIENT] [{0}] - Query: {1}", Session.User.UserName, text);
			}

			// Write debug message (Info level)
			if (Logger.IsInterestedIn(LogLevel.Debug)) {
				Logger.DebugFormat(this, "Query From User: {0}", Session.User.UserName);
				Logger.DebugFormat(this, "Query: {0}", text.Trim());
			}

			// Get the locking mechanism.
			LockingMechanism locker = Session.Connection.LockingMechanism;
			LockingMode lockMode = LockingMode.None;
			IQueryResponse[] response = null;
			try {
				try {
					// For simplicity - all database locking is now exclusive inside
					// a transaction.  This means it is not possible to execute
					// queries concurrently inside a transaction.  However, we are
					// still able to execute queries concurrently from different
					// connections.
					//
					// It's debatable whether we even need to perform this Lock anymore
					// because we could change the contract of this method so that
					// it is not thread safe.  This would require that the callee ensures
					// more than one thread can not execute queries on the connection.
					lockMode = LockingMode.Exclusive;
					locker.SetMode(lockMode);

					// Execute the Query (behaviour for this comes from super).
					response = DoExecuteQuery(text, parameters);

					// Return the result.
					return response;

				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						if (lockMode != LockingMode.None) {
							locker.FinishMode(lockMode);
						}
					} catch (Exception e) {
						// If this throws an exception, we should output it to the debug
						// log and screen.
						Logger.Error(this, "Exception finishing locks");
						Logger.Error(this, e);
						// Note, we can't throw an error here because we may already be in
						// an exception that happened in the above 'try' block.
					}
				}
			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the Query here.
				// Do we auto-commit?
				if (Session.Connection.AutoCommit) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						locker.SetMode(LockingMode.Exclusive);
						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							Session.Connection.Rollback();
						} else {
							try {
								// Otherwise commit.
								Session.Connection.Commit();
							} catch (Exception e) {
								foreach (IQueryResponse queryResponse in response) {
									// Dispose this response if the commit failed.
									DisposeResult(queryResponse.ResultId);
								}

								// And throw the SQL Exception
								throw;
							}
						}
					} finally {
						locker.FinishMode(LockingMode.Exclusive);
					}
				}
			}
		}

		private void ClearResults() {
			List<int> keys;
			lock (resultMap) {
				keys = new List<int>(resultMap.Keys);
			}

			foreach (int resultId in keys) {
				DisposeResult(resultId);
			}
		}

		public abstract IMessageProcessor CreateProcessor();

		public abstract IMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message);

		public abstract IStreamableObjectChannel CreateChannel(long objectId);

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				// Clear the result set mapping
				ClearResults();

				if (Session != null)
					Session.Dispose();
			}

			ChangeState(ConnectorState.Disposed);
		}

		private sealed class QueryResponse : IQueryResponse {
			private readonly QueryResult result;

			internal QueryResponse(int resultId, QueryResult result, int queryTime, string warnings) {
				ResultId = resultId;
				this.result = result;
				QueryTimeMillis = queryTime;
				Warnings = warnings;
			}

			public int ResultId { get; private set; }

			public int QueryTimeMillis { get; private set; }

			public int RowCount {
		        get { return result.RowCount; }
		    }

		    public int ColumnCount {
		        get { return result.ColumnCount; }
		    }

		    public ColumnDescription GetColumnDescription(int n) {
				return result.Fields[n];
			}

			public string Warnings { get; private set; }
		}
	}
}