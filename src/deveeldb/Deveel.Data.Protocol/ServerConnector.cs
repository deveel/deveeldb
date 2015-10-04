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
using System.Diagnostics;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Store;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.Protocol {
	public abstract class ServerConnector : IServerConnector {
		private bool autoCommit;
		private QueryParameterStyle parameterStyle;
		private bool ignoreIdentifiersCase;

		private Dictionary<int, QueryResult> resultMap;
		private int uniqueResultId;

		protected ServerConnector(IDatabaseHandler databaseHandler) {
			DatabaseHandler = databaseHandler;

			resultMap = new Dictionary<int, QueryResult>();
			uniqueResultId = -1;
		}

		~ServerConnector() {
			Dispose(false);
		}

		protected IDatabaseHandler DatabaseHandler { get; private set; }

		protected IDatabase Database { get; private set; }

		protected IQueryContext QueryContext { get; private set; }

		public void Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				ClearResults();

				if (QueryContext != null)
					QueryContext.Dispose();
			}

			QueryContext = null;
			DatabaseHandler = null;

			ChangeState(ConnectorState.Disposed);
		}

		public abstract ConnectionEndPoint LocalEndPoint { get; }

		public ConnectionEndPoint RemoteEndPoint { get; private set; }

		public ConnectorState CurrentState { get; private set; }

		private void AssertNotDisposed() {
			if (CurrentState == ConnectorState.Disposed)
				throw new ObjectDisposedException(GetType().AssemblyQualifiedName);
		}

		private void AssertAuthenticated() {
			if (CurrentState != ConnectorState.Authenticated)
				throw new InvalidOperationException("The connector is not authenticated.");
		}

		protected void SetAutoCommit(bool value) {
			QueryContext.AutoCommit(value);
		}

		protected void SetParameterStyle(QueryParameterStyle parameterStyle) {
			QueryContext.ParameterStyle(parameterStyle);
		}

		protected void SetIgnoreIdentifiersCase(bool value) {
			QueryContext.IgnoreIdentifiersCase(value);
		}

		private void ClearResults() {
			lock (resultMap) {
				foreach (var result in resultMap.Values) {
					if (result != null)
						result.Dispose();
				}

				resultMap.Clear();
			}
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
					throw new InvalidOperationException();

				OnConnectorOpen();
				ChangeState(ConnectorState.Open);
			} catch (Exception ex) {
				// TODO: Log the error...
				throw;
			}
		}

		protected virtual void OnConnectorOpen() {
		}

		public abstract ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties);

		protected void CloseConnector() {
			try {
				OnCloseConnector();
			} catch (Exception ex) {
				// TODO: log the exception
			} finally {
				ChangeState(ConnectorState.Closed);
			}
		}

		protected virtual void OnCloseConnector() {
		}

		protected virtual EncryptionData GetEncryptionData() {
			return null;
		}

		protected virtual bool Authenticate(string defaultSchema, string username, string password) {
			if (CurrentState == ConnectorState.Authenticated &&
				QueryContext != null)
				throw new InvalidOperationException("Already authenticated.");

			// TODO: Log a debug information

			// TODO: Log an information about the logging user...

			try {
				QueryContext = OnAuthenticate(defaultSchema, username, password);
				if (QueryContext == null)
					return false;

				QueryContext.AutoCommit(autoCommit);
				QueryContext.IgnoreIdentifiersCase(ignoreIdentifiersCase);
				QueryContext.ParameterStyle(parameterStyle);

				ChangeState(ConnectorState.Authenticated);

				return true;
			} catch (Exception e) {
				// TODO: throw server error
				throw;
			}
		}

		protected virtual IQueryContext OnAuthenticate(string defaultSchema, string username, string password) {
			var user = Database.Authenticate(username, password);

			if (user == null)
				return null;

			var connection = Database.CreateUserSession(user);

			// Put the connection in exclusive mode
			connection.ExclusiveLock();

			var context = new SessionQueryContext(connection);

			try {
				// By default, connections are auto-commit
				connection.AutoCommit(true);

				// Set the default schema for this connection if it exists
				if (context.SchemaExists(defaultSchema)) {
					context.CurrentSchema(defaultSchema);
				} else {
					// TODO: Log the warning..

					// If we can't change to the schema then change to the APP schema
					connection.CurrentSchema(Database.DatabaseContext.DefaultSchema());
				}
			} finally {
				try {
					connection.Commit();
				} catch (TransactionException e) {
					// TODO: Log the warning
				}
			}

			return context;
		}

		private IQueryContext OpenQueryContext(long commitId) {
			var sessionInfo = QueryContext.Session.SessionInfo;
			var transaction = Database.TransactionFactory.OpenTransactions.FindById((int)commitId);
			if (transaction == null)
				throw new InvalidOperationException();

			var session = new UserSession(Database, transaction, sessionInfo);
			return new SessionQueryContext(session);
		}

		protected IQueryResponse[] ExecuteQuery(long commitId, string text, IEnumerable<QueryParameter> parameters) {
			AssertAuthenticated();

			IQueryContext queryContext = QueryContext;
			if (commitId > 0 && commitId != QueryContext.Session.Transaction.CommitId) {
				queryContext = OpenQueryContext(commitId);
			}

			return ExecuteQuery(queryContext, text, parameters);
		}

		protected virtual IQueryResponse[] ExecuteQuery(IQueryContext context, string text, IEnumerable<QueryParameter> parameters) {
			// TODO: Log a debug message..

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
					context.Session.ExclusiveLock();

					// Execute the Query (behaviour for this comes from super).
					response = CoreExecuteQuery(context, text, parameters);

					// Return the result.
					return response;

				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						context.Session.ReleaseLocks();
					} catch (Exception e) {
						// TODO: Log errors ...
					}
				}
			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the Query here.
				// Do we auto-commit?
				if (context.AutoCommit()) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						context.Session.ExclusiveLock();

						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							context.Session.Rollback();
						} else {
							try {
								// Otherwise commit.
								QueryContext.Session.Commit();
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
						context.Session.ReleaseLocks();
					}
				}
			}
		}

		protected IQueryResponse[] CoreExecuteQuery(IQueryContext context, string text, IEnumerable<QueryParameter> parameters) {
			// Where Query result eventually resides.
			int resultId = -1;

			// For each StreamableObject in the query object, translate it to a
			// IRef object that presumably has been pre-pushed onto the server from
			// the client.

			// Evaluate the sql Query.
			var query = new SqlQuery(text);
			if (parameters != null) {
				// TODO: Download the Large-Objects and replace with a reference
			}

			var stopwatch = new Stopwatch();
			stopwatch.Start();

			var results = context.ExecuteQuery(query);
			var responses = new IQueryResponse[results.Length];
			int j = 0;

			foreach (var result in results) {
				QueryResult queryResult;
				try {
					queryResult = new QueryResult(query, result);
					resultId = AddResult(queryResult);
				} catch (Exception e) {
					if (resultId != -1)
						DisposeResult(resultId);

					throw;
				}

				var taken = stopwatch.Elapsed;

				// Return the Query response
				responses[j] = new QueryResponse(resultId, queryResult, (int)taken.TotalMilliseconds, "");

				j++;
			}

			stopwatch.Stop();
			return responses;
		}

		private void DisposeResult(int resultId) {
			// Remove this entry.
			QueryResult result;
			lock (resultMap) {
				if (resultMap.TryGetValue(resultId, out result))
					resultMap.Remove(resultId);
			}

			if (result != null) {
				result.Dispose();
			} else {
				// TODO: Log an error ...
			}
		}

		private int AddResult(QueryResult result) {
			result.LockRoot(-1); // -1 because lock_key not implemented

			int resultId;

			lock (resultMap) {
				resultId = ++uniqueResultId;
				resultMap[resultId] = result;
			}

			return resultId;
		}

		protected int BeginTransaction() {
			AssertAuthenticated();

			var transaction = Database.CreateTransaction(IsolationLevel.Serializable);
			if (transaction == null)
				throw new InvalidOperationException();

			return transaction.CommitId;
		}

		IMessageProcessor IConnector.CreateProcessor() {
			return new ServerMessageProcessor(this);
		}

		protected abstract IServerMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message);

		IMessageEnvelope IConnector.CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			return CreateEnvelope(metadata, message);
		}


		public ILargeObjectChannel CreateObjectChannel(long objectId) {
			throw new NotImplementedException();
		}

		public ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			throw new NotImplementedException();
		}

		protected virtual IMessage GetMessage(IMessageEnvelope envelope) {
			if (envelope == null)
				return null;

			// TODO: handle errors? it's not supposed the client to send errors to the server ...

			return envelope.Message;
		}

		protected void CommitTransaction(int commitId) {
			AssertNotDisposed();

			var transaction = Database.TransactionFactory.OpenTransactions.FindById(commitId);
			if (transaction == null)
				throw new InvalidOperationException();

			var sessionInfo = QueryContext.Session.SessionInfo;
			using (var session = new UserSession(Database, transaction, sessionInfo)) {
				session.Commit();
			}
		}

		protected void RollbackTransaction(int commitId) {
			AssertNotDisposed();

			var transaction = Database.TransactionFactory.OpenTransactions.FindById(commitId);
			if (transaction == null)
				throw new InvalidOperationException();

			var sessionInfo = QueryContext.Session.SessionInfo;
			using (var session = new UserSession(Database, transaction, sessionInfo)) {
				session.Rollback();
			}
		}

		protected QueryResultPart GetResultPart(int resultId, int startRow, int countRows) {
			AssertNotDisposed();

			var table = GetResult(resultId);
			if (table == null)
				throw new InvalidOperationException();

			int rowEnd = startRow + countRows;

			if (startRow < 0 || startRow >= table.RowCount || rowEnd > table.RowCount) {
				throw new InvalidOperationException("Result part out of range.");
			}

			try {
				int colCount = table.ColumnCount;
				var block = new QueryResultPart(colCount);
				for (int r = startRow; r < rowEnd; ++r) {
					var row = new ISqlObject[colCount];
					var sizes = new int[colCount];

					for (int c = 0; c < colCount; ++c) {
						var value = table.GetValue(r, c);

						ISqlObject clientOb = null;
						if (value.Value is IObjectRef) {
							var reference = (IObjectRef)value.Value;
							// TODO: Make a protocol placeholder for the large object ref
						} else {
							clientOb = value.Value;
						}

						row[c] = clientOb;
						sizes[c] = value.Size;
					}

					block.AddRow(new QueryResultRow(row, sizes));
				}
				return block;
			} catch (Exception e) {
				// TODO: Log a warning ...
				throw;
			}
		}

		protected QueryResult GetResult(int resultId) {
			lock (resultMap) {
				QueryResult result;
				if (!resultMap.TryGetValue(resultId, out result))
					return null;

				return null;
			}
		}

		private ObjectId CreateLargeObject(long objectLength) {
			throw new NotImplementedException();
		}

		#region QueryResponse

		class QueryResponse : IQueryResponse {
			public QueryResponse(int resultId, QueryResult queryResult, int millis, string warnings) {
				ResultId = resultId;
				QueryResult = queryResult;
				QueryTimeMillis = millis;
				Warnings = warnings;
			}

			public int ResultId { get; private set; }

			public QueryResult QueryResult { get; set; }

			public int QueryTimeMillis { get; private set; }

			public int RowCount {
				get { return QueryResult.RowCount; }
			}

			public int ColumnCount {
				get { return QueryResult.ColumnCount; }
			}

			public QueryResultColumn GetColumn(int column) {
				return QueryResult.GetColumn(column);
			}

			public string Warnings { get; private set; }
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
					return ProcessConnect(metadata, (ConnectRequest)message);

				if (message is AuthenticateRequest)
					return ProcessAuthenticate(metadata, (AuthenticateRequest)message);

				if (message is QueryExecuteRequest)
					return ProcessQuery(metadata, (QueryExecuteRequest)message);
				if (message is QueryResultPartRequest)
					return ProcessQueryPart(metadata, (QueryResultPartRequest)message);
				if (message is DisposeResultRequest)
					return ProcessDisposeResult(metadata, (DisposeResultRequest)message);

				if (message is LargeObjectCreateRequest)
					return ProcessCreateLargeObject(metadata, (LargeObjectCreateRequest)message);

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
					// TODO: Log the error ...
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
					// TODO: Log the error ...
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessQuery(IDictionary<string, object> metadata, QueryExecuteRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					// TODO: use the timeout ...
					var queryResonse = connector.ExecuteQuery(request.CommitId, request.Query.Text, request.Query.Parameters);
					return connector.CreateEnvelope(metadata, new QueryExecuteResponse(queryResonse));
				} catch (Exception ex) {
					// TODO: Log the error ...
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
					// TODO: Log the error ...
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessDisposeResult(IDictionary<string, object> metadata, DisposeResultRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					connector.DisposeResult(request.ResultId);
					return connector.CreateEnvelope(metadata, new AcknowledgeResponse(true));
				} catch (Exception ex) {
					// TODO: Log the error ...
					return CreateErrorResponse(metadata, ex);
				}
			}

			private IMessageEnvelope ProcessCreateLargeObject(IDictionary<string, object> metadata,
				LargeObjectCreateRequest request) {
				try {
					connector.AssertNotDisposed();
					connector.AssertAuthenticated();

					var objRef = connector.CreateLargeObject(request.ObjectLength);
					return connector.CreateEnvelope(metadata, new LargeObjectCreateResponse(request.ObjectLength, objRef));
				} catch (Exception ex) {
					// TODO: Log the error ...
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
					// TODO: Log the error ...
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
					// TODO: Log the error ...
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
					// TODO: Log the error ...
					return CreateErrorResponse(metadata, ex);
				}
			}
		}

		#endregion
	}
}
