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
	public abstract class ServerConnector : IServerConnector {
		private readonly Dictionary<long, IRef> blobIdMap;

		private bool autoCommit;
		private bool ignoreIdentifiersCase;
		private ParameterStyle parameterStyle;

		private int triggerId;
		private Dictionary<int, TriggerChannel> triggerChannels;
		private readonly object triggerLock = new object();

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

		private void AssertAuthenticated() {
			if (CurrentState != ConnectorState.Authenticated)
				throw new InvalidOperationException("The connector is not authenticated.");
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
			} catch (Exception ex) {
				Logger.Error(this, "Error when closing the connector.");
				Logger.Error(this, ex);
			} finally {
				ChangeState(ConnectorState.Closed);
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
			lock (triggerChannels) {
				foreach (var channel in triggerChannels.Values) {
					if (channel.ShouldNotify(triggerName, triggerSource, eventType))
						channel.Notify(triggerName, triggerSource, eventType, count);
				}
			}
		}

		protected int BeginTransaction() {
			AssertNotDisposed();

			// TODO: In a future version, we will provide multiple transactions.
			//       for the moment we only set the current connection not to auto-commit
			//       that will require an explicit commit.
			Session.Connection.AutoCommit = false;
			return -1;
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

		protected IQueryResponse[] CoreExecuteQuery(string text, IEnumerable<SqlQueryParameter> parameters) {
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

		protected virtual IQueryResponse[] ExecuteQuery(string text, IEnumerable<SqlQueryParameter> parameters) {
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
					response = CoreExecuteQuery(text, parameters);

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

		protected void CommitTransaction(int transactionId) {
			AssertNotDisposed();

			try {
				Session.Connection.Commit();
			} finally {
				Session.Connection.AutoCommit = autoCommit;
			}
		}

		protected void RollbackTransaction(int transactionId) {
			AssertNotDisposed();

			try {
				Session.Connection.Rollback();
			} finally {
				Session.Connection.AutoCommit = autoCommit;
			}
		}

		public abstract ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties);

		IMessageProcessor IConnector.CreateProcessor() {
			return new ServerMessageProcessor(this);
		}

		protected abstract IServerMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message);

		IMessageEnvelope IConnector.CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			return CreateEnvelope(metadata, message);
		}

		protected virtual IMessage GetMessage(IMessageEnvelope envelope) {
			if (envelope == null)
				return null;

			// TODO: handle errors? it's not supposed the client to send errors to the server ...

			return envelope.Message;
		}

		IStreamableObjectChannel IConnector.CreateObjectChannel(long objectId) {
			return CreateObjectChannel(objectId);
		}

		protected virtual IStreamableObjectChannel CreateObjectChannel(long objectId) {
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

		ITriggerChannel IConnector.CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			return CreateTriggerChannel(triggerName, objectName, eventType);
		}

		protected virtual ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			AssertAuthenticated();

			lock (triggerLock) {
				if (triggerChannels == null)
					triggerChannels = new Dictionary<int, TriggerChannel>();

				foreach (TriggerChannel channel in triggerChannels.Values) {
					// If there's an open channel for the trigger return it
					if (channel.ShouldNotify(triggerName, objectName, eventType))
						return channel;
				}

				int id = ++triggerId;
				var newChannel = new TriggerChannel(this, id, triggerName, objectName, eventType);
				triggerChannels[id] = newChannel;
				return newChannel;
			}
		}

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

		#region QueryResponse

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

		#endregion

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

		#region ServerMessageProcessor

		private class ServerMessageProcessor : IMessageProcessor {
			private readonly ServerConnector connector;

			public ServerMessageProcessor(ServerConnector connector) {
				this.connector = connector;
			}

			private IMessageEnvelope CreateErrorResponse(IMessageEnvelope sourceMessage, string message) {
				return CreateErrorResponse(sourceMessage, new ProtocolException(message));
			}

			private IMessageEnvelope CreateErrorResponse(IMessageEnvelope sourceMessage, Exception error) {
				IDictionary<string, object> metadata = null;
				if (sourceMessage != null)
					metadata = sourceMessage.Metadata;

				return CreateErrorResponse(metadata, error);
			}

			private IMessageEnvelope CreateErrorResponse(IDictionary<string, object> metadata, Exception error) {
				var envelope = connector.CreateEnvelope(metadata, new AcknowledgeResponse(false));
				envelope.SetError(error);
				return envelope;
			}

			private IMessageEnvelope ProcessAuthenticate(IDictionary<string, object> metadata, AuthenticateRequest request) {
				try {
					if (!connector.Authenticate(request.DefaultSchema, request.UserName, request.Password)) {
						var response = connector.CreateEnvelope(metadata, new AuthenticateResponse(false, -1));
						// TODO: make the specialized exception ...
						response.SetError(new Exception("Unable to authenticate."));
						return response;
					}

					connector.ChangeState(ConnectorState.Authenticated);

					// TODO: Get the UNIX epoch here?
					return connector.CreateEnvelope(metadata, new AuthenticateResponse(true, DateTime.UtcNow.Ticks));
				} catch (Exception ex) {
					return CreateErrorResponse(metadata, ex);
				}
			}

			public IMessageEnvelope ProcessMessage(IMessageEnvelope envelope) {
				var metadata = envelope.Metadata;
				var message = connector.GetMessage(envelope);
				if (message == null)
					return CreateErrorResponse(metadata, new Exception("No message found in the envelope."));

				if (message is ConnectRequest)
					return ProcessConnect(metadata, (ConnectRequest) message);

				if (message is AuthenticateRequest)
					return ProcessAuthenticate(metadata, (AuthenticateRequest) message);

				if (message is QueryExecuteRequest)
					return ProcessQuery(metadata, (QueryExecuteRequest) message);
				if (message is QueryResultPartRequest)
					return ProcessQueryPart(metadata, (QueryResultPartRequest) message);
				if (message is DisposeResultRequest)
					return ProcessDisposeResult(metadata, (DisposeResultRequest) message);

				if (message is LargeObjectCreateRequest)
					return ProcessCreateLargeObject(metadata, (LargeObjectCreateRequest) message);

				if (message is BeginRequest)
					return ProcessBegin(metadata);
				if (message is CommitRequest)
					return ProcessCommit(metadata, (CommitRequest)message);
				if (message is RollbackRequest)
					return ProcessRollback(metadata, (RollbackRequest)message);

				if (message is CloseRequest)
					return ProcessClose(metadata);

				return CreateErrorResponse(envelope, "Message not supported");
			}

			private IMessageEnvelope ProcessConnect(IDictionary<string, object> metadata, ConnectRequest request) {
				Exception error = null;
				ConnectResponse response;

				try {
					connector.OpenConnector(request.RemoteEndPoint, request.DatabaseName);
					if (request.AutoCommit)
						connector.SetAutoCommit(request.AutoCommit);

					connector.SetIgnoreIdentifiersCase(request.IgnoreIdentifiersCase);
					connector.SetParameterStyle(request.ParameterStyle);

					var encryptionData = connector.GetEncryptionData();

					var serverVersion = connector.Database.Version.ToString(2);
					response = new ConnectResponse(true, serverVersion, encryptionData != null, encryptionData);
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while opening a connection.");
					connector.Logger.Error(connector, ex);

					error = ex;
					response = new ConnectResponse(false, null);
				}

				var envelope = connector.CreateEnvelope(metadata, response);
				if (error != null)
					envelope.SetError(error);

				return connector.CreateEnvelope(metadata, response);
			}

			private IMessageEnvelope ProcessClose(IDictionary<string, object> metadata) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					connector.CloseConnector();
					return connector.CreateEnvelope(metadata, new AcknowledgeResponse(true));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while closing a connection.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessQuery(IDictionary<string, object> metadata, QueryExecuteRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					// TODO: use the timeout ...
					var queryResonse = connector.ExecuteQuery(request.Query.Text, request.Query.Parameters);
					return connector.CreateEnvelope(metadata, new QueryExecuteResponse(queryResonse));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while processing a query request.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessQueryPart(IDictionary<string, object> metadata, QueryResultPartRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					var part = connector.GetResultPart(request.ResultId, request.RowIndex, request.Count);
					return connector.CreateEnvelope(metadata, new QueryResultPartResponse(request.ResultId, part));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while requesting part of a query result.");
					connector.Logger.Error(connector, ex);
					throw;
				}
			}

			private IMessageEnvelope ProcessDisposeResult(IDictionary<string, object> metadata, DisposeResultRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					connector.DisposeResult(request.ResultId);
					return connector.CreateEnvelope(metadata, new AcknowledgeResponse(true));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error occurred while disposing a query result.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessCreateLargeObject(IDictionary<string, object> metadata,
				LargeObjectCreateRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					var objRef = connector.CreateStreamableObject(request.ReferenceType, request.ObjectLength);
					return connector.CreateEnvelope(metadata,
						new LargeObjectCreateResponse(request.ReferenceType, request.ObjectLength, objRef));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while creating a large object.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessBegin(IDictionary<string, object> metadata) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					var id = connector.BeginTransaction();
					return connector.CreateEnvelope(metadata, new BeginResponse(id));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while beginning a transaction.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessCommit(IDictionary<string, object> metadata, CommitRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					connector.CommitTransaction(request.TransactionId);
					return connector.CreateEnvelope(metadata, new AcknowledgeResponse(true));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while committing the transaction.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessRollback(IDictionary<string, object> metadata, RollbackRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					connector.RollbackTransaction(request.TransactionId);
					return connector.CreateEnvelope(metadata, new AcknowledgeResponse(true));
				} catch (Exception ex) {
					connector.Logger.Error(connector, "Error while rolling-back the transaction.");
					connector.Logger.Error(connector, ex);
					return CreateErrorResponse(metadata, ex);
				}				
			}
		}

		#endregion

		#region TriggerChannel

		class TriggerChannel : ITriggerChannel {
			private readonly ServerConnector connector;
			private readonly long id;

			private string TriggerName { get; set; }

			private string ObjectName { get; set; }

			private TriggerEventType EventType { get; set; }

			private Action<TriggerEventNotification> callback; 

			public TriggerChannel(ServerConnector connector, long id, string triggerName, string objectName, TriggerEventType eventType) {
				this.connector = connector;
				this.id = id;
				TriggerName = triggerName;
				ObjectName = objectName;
				EventType = eventType;
			}

			public bool ShouldNotify(string triggerName, string objectName, TriggerEventType eventType) {
				if (!String.Equals(triggerName, TriggerName, StringComparison.OrdinalIgnoreCase))
					return false;

				return (eventType & EventType) != 0;
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					connector.DisposeTriggerChannel(id);
				}
			}

			public void OnTriggeInvoked(Action<TriggerEventNotification> notification) {
				callback = notification;
			}

			public void Notify(string triggerName, string triggerSource, TriggerEventType eventType, int count) {
				if (callback != null)
					callback(new TriggerEventNotification(triggerName, triggerSource, TriggerType.Callback, eventType, count));
			}
		}

		private void DisposeTriggerChannel(long id) {
			throw new NotImplementedException();
		}

		#endregion
	}
}