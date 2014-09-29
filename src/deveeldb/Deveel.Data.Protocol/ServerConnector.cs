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
using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	public abstract class ServerConnector : IConnector {
		private readonly Dictionary<long, IRef> blobIdMap;

		private bool autoCommit;
		private bool ignoreIdentifiersCase;
		private ParameterStyle parameterStyle;

		protected ServerConnector(IDatabaseHandler handler) {
			if (handler == null)
				throw new ArgumentNullException("handler");

			DatabaseHandler = handler;
			resultMap = new Dictionary<int, QueryResult>();
			blobIdMap = new Dictionary<long, IRef>();
			uniqueResultId = 1;
		}

		public AuthenticatedSession Session { get; protected set; }

		public abstract ConnectionEndPoint LocalEndPoint { get; }

		public ConnectionEndPoint RemoteEndPoint { get; private set; }

		public ConnectorState CurrentState { get; private set; }

		protected IDatabaseHandler DatabaseHandler { get; private set; }

		protected ILogger Logger {
			get {
				if (Database == null)
					return new EmptyLogger();

				return Database.Context.Logger;
			}
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

		protected void OpenConnector(ConnectionEndPoint remoteEndPoint, string databaseName) {
			try {
				RemoteEndPoint = remoteEndPoint;
				Database = DatabaseHandler.GetDatabase(databaseName);
				if (Database == null)
					throw new DatabaseException();

				OnConnectorOpen();
				ChangeState(ConnectorState.Open);
			} catch (Exception ex) {
				Logger.Error(this, "Error when opening the connector.");
				Logger.Error(this, ex);
				throw;
			}
		}

		protected virtual void OnConnectorOpen() {
		}

		protected void SetAutoCommit(bool state) {
			AssertNotDisposed();
			autoCommit = state;
		}

		protected void SetIgnoreIdentifiersCase(bool state) {
			AssertNotDisposed();
			ignoreIdentifiersCase = state;
		}

		protected void SetParameterStyle(ParameterStyle style) {
			AssertNotDisposed();
			parameterStyle = style;
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

		protected virtual AuthenticatedSession OnAuthenticate(string defaultSchema, string username, string password) {
			var user = Database.AuthenticateUser(username, password, RemoteEndPoint);

			if (user == null) 
				return null;

			IDatabaseConnection connection = Database.CreateNewConnection(user, OnTriggerFired);

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

			return new AuthenticatedSession(user, connection);
		}

		protected virtual void OnTriggerFired(string triggerName, string triggerSource, TriggerEventType eventType, int count) {
		}

		protected void BeginTransaction() {
			AssertNotDisposed();

			Session.Connection.AutoCommit = false;
		}

		protected virtual bool Authenticate(string defaultSchema, string username, string password) {
			if (CurrentState == ConnectorState.Authenticated &&
			    Session != null)
				throw new InvalidOperationException("Already authenticated.");

			if (Logger.IsInterestedIn(LogLevel.Debug)) {
				// Output the instruction to the _queries log.
				Logger.DebugFormat(this, "[CLIENT] [{0}] - Log in", username);
			}

			if (Logger.IsInterestedIn(LogLevel.Info)) {
				Logger.InfoFormat(this, "Authenticate User: {0}", username);
			}

			try {
				Session = OnAuthenticate(defaultSchema, username, password);
				if (Session == null)
					return false;

				Session.Connection.AutoCommit = autoCommit;
				Session.Connection.IsInCaseInsensitiveMode = ignoreIdentifiersCase;
				Session.Connection.ParameterStyle = parameterStyle;

				ChangeState(ConnectorState.Authenticated);

				return true;
			} catch (Exception e) {
				// TODO: throw server error
				throw;
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

		private IRef GetObjectRef(long objectId) {
			lock (blobIdMap) {
				IRef obj;
				if (!blobIdMap.TryGetValue(objectId, out obj)) {
					obj = Session.Connection.GetLargeObject(objectId);
					blobIdMap[objectId] = obj;
				}

				return obj;
			}
		}

		protected virtual IQueryResponse[] ExecuteQuery(string text, IEnumerable<SqlQueryParameter> parameters) {
			// Record the Query start time
			DateTime startTime = DateTime.Now;

			// Where Query result eventually resides.
			int resultId = -1;

			// For each StreamableObject in the query object, translate it to a
			// IRef object that presumably has been pre-pushed onto the server from
			// the client.

			// Evaluate the sql Query.
			var query = new SqlQuery(text);
			if (parameters != null) {
				foreach (var parameter in parameters) {
					var preparedParam = parameter.Value;
					if (preparedParam is StreamableObject) {
						var obj = (StreamableObject) preparedParam;
						IRef objRef = CompleteStream(obj.Identifier);
						preparedParam = objRef;
					}
					query.Parameters.Add(new SqlQueryParameter(parameter.Name, preparedParam));
				}
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

		private void ClearResults() {
			List<int> keys;
			lock (resultMap) {
				keys = new List<int>(resultMap.Keys);
			}

			foreach (int resultId in keys) {
				DisposeResult(resultId);
			}
		}

		protected void CommitTransaction() {
			AssertNotDisposed();

			try {
				Session.Connection.Commit();
			} catch (Exception) {

				throw;
			} finally {
				Session.Connection.AutoCommit = autoCommit;
			}
		}

		protected void RollbackTransaction() {
			AssertNotDisposed();

			try {
				Session.Connection.Rollback();
			} catch (Exception) {

				throw;
			} finally {
				Session.Connection.AutoCommit = autoCommit;
			}
		}

		public abstract ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties);

		public abstract IMessageProcessor CreateProcessor();

		public abstract IMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message);

		public IStreamableObjectChannel CreateObjectChannel(long objectId) {
			var obj = GetObjectRef(objectId);
			if (obj == null)
				throw new InvalidOperationException("The object was not created or was not found.");

			return new DirectStreamableObjectChannel(this, obj);
		}

		private void DisposeChannel(long objId) {
			lock (blobIdMap) {
				blobIdMap.Remove(objId);
			}
		}

		private IRef CompleteStream(long objId) {
			lock (blobIdMap) {
				var objRef = GetObjectRef(objId);
				if (objRef == null)
					throw new InvalidOperationException();

				blobIdMap.Remove(objId);
				objRef.Complete();
				return objRef;
			}
		}

		public abstract ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType);

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

		    public QueryResultColumn GetColumnDescription(int n) {
				return result.Fields[n];
			}

			public string Warnings { get; private set; }
		}

		#region DirectStreamableObjectChannel

		private class DirectStreamableObjectChannel : IStreamableObjectChannel {
			private readonly IRef obj;
			private readonly ServerConnector connector;

			public DirectStreamableObjectChannel(ServerConnector connector, IRef obj) {
				this.obj = obj;
				this.connector = connector;
			}

			public void Dispose() {
				connector.DisposeChannel(obj.Id);
			}

			public void PushData(long offset, byte[] buffer, int length) {
				obj.Write(offset, buffer, length);
			}

			public byte[] ReadData(long offset, int length) {
				if (length > 512 * 1024)
					throw new DatabaseException("Request length exceeds 512 KB");

				try {
					// Read the blob part into the byte array.
					var blobPart = new byte[length];
					obj.Read(offset, blobPart, length);

					// And return as a StreamableObjectPart object.
					return blobPart;
				} catch (IOException e) {
					throw new DatabaseException("Exception while reading blob: " + e.Message, e);
				}
			}
		}

		#endregion
	}
}