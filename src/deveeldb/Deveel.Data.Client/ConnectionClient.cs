using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Remoting;

using Deveel.Data.Configuration;
using Deveel.Data.Protocol;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.Client {
	internal class ConnectionClient : IDisposable {
		public DeveelDbConnectionStringBuilder Settings { get; private set; }

		private ConnectionEndPoint remoteEndPoint;

		public IClientConnector Connector { get; private set; }

		private bool OwnsConnector { get; set; }

		public IMessageProcessor Processor { get; private set; }

		private IDictionary<string, object> serverMetadata;

		public ConnectionClient(DeveelDbConnectionStringBuilder settings) {
			Settings = settings;
		}

		public ConnectionClient(IClientConnector connector) {
			if (connector == null)
				throw new ArgumentNullException("connector");

			Connector = connector;
			Processor = connector.CreateProcessor();
			OwnsConnector = false;
		}

		~ConnectionClient() {
			Dispose(false);
		}

		public bool IsClosed { get; private set; }

		public string ServerVersion { get; private set; }

		private IClientConnector CreateNetworkConnector() {
			throw new NotSupportedException();
		}

		private IClientConnector CreateConnector() {
			if (Connector != null)
				return Connector;

			IDbConfig dbConfig = null;

			/*
			TODO:
			if (IsInMemory(Settings.DataSource) && controlDatabase == null) {
				if (controlSystem == null)
					controlSystem = CreateEmbeddedControlSystem();

				// TODO: handle the case the connection string does not specify a database name
				var databaseName = Settings.Database;
				if (String.IsNullOrEmpty(databaseName))
					throw new InvalidOperationException();

				dbConfig = new DbConfig(controlSystem.Config);
				dbConfig.DatabaseName(databaseName);

				var defaultSchema = Settings.Schema;
				if (!String.IsNullOrEmpty(defaultSchema))
					dbConfig.DefaultSchema(defaultSchema);

				controlDatabase = controlSystem.ControlDatabase(databaseName);
			} else if (IsInFileSystem(Settings) && controlDatabase == null) {
				if (controlSystem == null)
					controlSystem = CreateEmbeddedControlSystem();

				// TODO: handle the case the connection string does not specify a database name
				var databaseName = Settings.Database;
				if (String.IsNullOrEmpty(databaseName))
					throw new InvalidOperationException();

				dbConfig = new DbConfig(controlSystem.Config);
				dbConfig.StorageSystem(ConfigDefaultValues.FileStorageSystem);

				var dbPath = settings.DataSource;
				if (String.Equals(dbPath, "local", StringComparison.OrdinalIgnoreCase))
					dbPath = settings.Path;
				if (String.IsNullOrEmpty(dbPath))
					dbPath = databaseName;

				dbConfig.DatabasePath(dbPath);

				var defaultSchema = settings.Schema;
				if (!String.IsNullOrEmpty(defaultSchema))
					dbConfig.DefaultSchema(defaultSchema);


				controlDatabase = controlSystem.ControlDatabase(databaseName);
			} else if (controlDatabase == null) {
				return CreateNetworkConnector();
			}

			if (controlDatabase != null)
				return CreateLocalDatabaseConnector(dbConfig);

			throw new InvalidOperationException("Unable to create a connector to the database");
			*/

			throw new NotImplementedException();
		}

		private IMessage SendMessage(IMessage message) {
			var envelope = Connector.CreateEnvelope(serverMetadata, message);
			var response = Processor.ProcessMessage(envelope);
			if (response == null)
				throw new InvalidOperationException("The processor returned no response.");

			if (response.Error != null)
				throw new ServerException(response.Error.ErrorMessage);

			serverMetadata = response.Metadata;
			return response.Message;
		}

		private ConnectionEndPoint MakeRemoteEndPoint() {
			var properties = new Dictionary<string, object>();
			var en = ((IDictionary)Settings).GetEnumerator();
			while (en.MoveNext()) {
				var current = en.Entry;
				properties.Add((string)current.Key, current.Value);
			}

			return Connector.MakeEndPoint(properties);
		}

		public void Connect() {
			if (Connector == null) {
				Connector = CreateConnector();
				Processor = Connector.CreateProcessor();
			}

			remoteEndPoint = MakeRemoteEndPoint();

			var request = new ConnectRequest(Connector.LocalEndPoint, remoteEndPoint) {
				DatabaseName = Settings.Database,
				Timeout = Settings.QueryTimeout,
				IgnoreIdentifiersCase = Settings.IgnoreIdentifiersCase,
				ParameterStyle = Settings.ParameterStyle,
				AutoCommit = Settings.AutoCommit
			};

			var response = SendMessage(request) as ConnectResponse;
			if (response == null)
				throw new ProtocolException("The returned message is invalid");

			if (!response.Opened)
				throw new ServerException("Was not able to open the connection on the server.");

			if (response.IsEncryted)
				Connector.SetEncrypton(response.EncryptionData);

			IsClosed = false;
			ServerVersion = response.Version;
		}

		public void Disconnect() {
			try {
				var response = SendMessage(new CloseRequest())
					as AcknowledgeResponse;

				if (response == null)
					throw new InvalidOperationException();

				if (!response.State)
					throw new ServerException("Unable to close the connection on the server.");
			} finally {
				IsClosed = true;
			}
		}

		public bool Authenticate() {
			var response = SendMessage(new AuthenticateRequest(Settings.Schema, Settings.UserName, Settings.Password))
				as AuthenticateResponse;

			if (response == null)
				throw new InvalidOperationException();

			return response.Authenticated;
		}

		public IQueryResponse[] ExecuteQuery(int commitId, SqlQuery query) {
			var response = SendMessage(new QueryExecuteRequest(commitId, query))
				as QueryExecuteResponse;

			if (response == null)
				throw new InvalidOperationException();

			return response.QueryResponse;
		}

		public QueryResultPart GetResultPart(int resultId, int rowIndex, int count) {
			var response = SendMessage(new QueryResultPartRequest(resultId, rowIndex, count))
				as QueryResultPartResponse;

			if (response == null)
				throw new InvalidOperationException();

			return response.Part;
		}

		public void DisposeResult(int resultId) {
			var response = SendMessage(new DisposeResultRequest(resultId))
				as AcknowledgeResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (!response.State)
				throw new ServerException();
		}

		public int BeginTransaction(IsolationLevel isolationLevel) {
			var isolation = MapIsolationLevel(isolationLevel);
			return BeginTransaction(isolation);
		}

		private TransactionIsolation MapIsolationLevel(IsolationLevel isolationLevel) {
			if (isolationLevel == IsolationLevel.Serializable)
				return TransactionIsolation.Serializable;
			if (isolationLevel == IsolationLevel.Snapshot)
				return TransactionIsolation.Snapshot;
			if (isolationLevel == IsolationLevel.ReadCommitted)
				return TransactionIsolation.ReadCommitted;
			if (isolationLevel == IsolationLevel.ReadUncommitted)
				return TransactionIsolation.ReadUncommitted;

			throw new NotSupportedException(String.Format("Isolation Level '{0}' not supported by DeveelDB", isolationLevel));
		}

		public int BeginTransaction(TransactionIsolation isolationLevel) {
			var response = SendMessage(new BeginRequest(isolationLevel))
				as BeginResponse;

			if (response == null)
				throw new InvalidOperationException();

			return response.CommitId;
		}

		public void CommitTransaction(int transactionId) {
			var response = SendMessage(new CommitRequest(transactionId))
				as AcknowledgeResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (!response.State)
				throw new ServerException("Unable to commit the transaction on the server.");
		}

		public void RollbackTransaction(int transactionId) {
			var response = SendMessage(new RollbackRequest(transactionId))
				as AcknowledgeResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (!response.State)
				throw new ServerException("Unable to rollback the transaction on the server.");

		}

		public void DisposeLargeObject(long objId) {
			var response = SendMessage(new LargeObjectDisposeRequest(objId))
				as AcknowledgeResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (!response.State)
				throw new InvalidOperationException("Unable to dispose the large object on the server.");
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (OwnsConnector && Connector != null) {
					Connector.Dispose();
				}
			}

			Connector = null;
		}
	}
}
