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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.Remoting;

using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.Protocol;
using Deveel.Data.Routines;

namespace Deveel.Data.Client {
	class ConnectionClient : IDisposable {
		private DeveelDbConnectionStringBuilder settings;

		private ConnectionEndPoint remoteEndPoint;

		private IClientConnector connector;
		private IMessageProcessor processor;
		private IControlDatabase controlDatabase;
		private IControlSystem controlSystem;

		private IDictionary<string, object> serverMetadata;

		public ConnectionClient(DeveelDbConnectionStringBuilder settings) {
			this.settings = settings;

			serverMetadata = null;
		}

		public bool IsClosed { get; private set; }

		public string ServerVersion { get; private set; }

		public void UpdateSettings(DeveelDbConnectionStringBuilder connectionString) {
			settings = connectionString;
		}

		private IClientConnector CreateNetworkConnector() {
			var host = settings.Host;
			
			// TODO: discover the protocol from the host ...

			return new TcpClientConnector();
		}

		private IClientConnector CreateConnector() {
			if (connector != null)
				return connector;

			IDbConfig dbConfig = null;

			if (IsInMemory(settings.DataSource) &&
				controlDatabase == null) {
				if (controlSystem == null)
					controlSystem = CreateEmbeddedControlSystem();

				// TODO: handle the case the connection string does not specify a database name
				var databaseName = settings.Database;
				if (String.IsNullOrEmpty(databaseName))
					throw new InvalidOperationException();

				dbConfig = new DbConfig(controlSystem.Config);
				dbConfig.DatabaseName(databaseName);

				var defaultSchema = settings.Schema;
				if (!String.IsNullOrEmpty(defaultSchema))
					dbConfig.DefaultSchema(defaultSchema);

				controlDatabase = controlSystem.ControlDatabase(databaseName);
			} else if (IsInFileSystem(settings) &&
			          controlDatabase == null) {
				if (controlSystem == null)
					controlSystem = CreateEmbeddedControlSystem();

				// TODO: handle the case the connection string does not specify a database name
				var databaseName = settings.Database;
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
		}

		private IClientConnector CreateLocalDatabaseConnector(IDbConfig dbConfig) {
			if (controlDatabase.CheckExists(dbConfig)) {
				if (controlDatabase.IsBooted) {
					return CreateClientServerConnector(controlDatabase.Connect(dbConfig));
				}
				if (settings.BootOrCreate) {
					return CreateClientServerConnector(controlDatabase.Boot(dbConfig));
				}
			} else if (settings.BootOrCreate) {
				return CreateClientServerConnector(controlDatabase.Create(dbConfig, settings.UserName, settings.Password));
			}

			throw new InvalidOperationException();
		}

		private IClientConnector CreateClientServerConnector(IServerConnector serverConnector) {
			return new ConnectionServerClientConnector(serverConnector);
		}

		#region ConnectionServerClientConnector

		class ConnectionServerClientConnector : ServerClientConnector {
			public ConnectionServerClientConnector(IServerConnector connector) 
				: base(connector) {
			}
		}

		#endregion

		private static bool IsInMemory(string dataSource) {
			return String.Equals(dataSource, "HEAP", StringComparison.OrdinalIgnoreCase) ||
			       String.Equals(dataSource, "MEMORY", StringComparison.OrdinalIgnoreCase);
		}

		private static bool IsInFileSystem(DeveelDbConnectionStringBuilder settings) {
			if (Path.IsPathRooted(settings.DataSource))
				return true;

			if (String.Equals(settings.Host, "local", StringComparison.OrdinalIgnoreCase) &&
			    !String.IsNullOrEmpty(settings.Path))
				return true;

			// TODO: handle more cases to identify is we're dealing with a file-based local database

			return false;
		}

		private IControlSystem CreateEmbeddedControlSystem() {
			var dbConfig = DbConfig.Default;

			if (IsInFileSystem(settings)) {
				var basePath = settings.Path;
				if (String.IsNullOrEmpty(basePath))
					basePath = settings.DataSource;
				if (String.IsNullOrEmpty(basePath))
					basePath = Environment.CurrentDirectory;

				dbConfig.BasePath(basePath);
				dbConfig.StorageSystem(ConfigDefaultValues.FileStorageSystem);
			} else if (IsInMemory(settings.DataSource)) {
				dbConfig.StorageSystem(ConfigDefaultValues.HeapStorageSystem);
			}

			var controller = DbController.Create(dbConfig);
			return new LocalSystem(controller);
		}

		private ConnectionEndPoint MakeRemoteEndPoint() {
			var properties = new Dictionary<string, object>();
			var en = ((IDictionary) settings).GetEnumerator();
			while (en.MoveNext()) {
				var current = en.Entry;
				properties.Add((string)current.Key, current.Value);
			}

			return connector.MakeEndPoint(properties);
		}

		private IMessage SendMessage(IMessage message) {
			var envelope = connector.CreateEnvelope(serverMetadata, message);
			var response = processor.ProcessMessage(envelope);
			if (response == null)
				throw new InvalidOperationException("The processor returned no response.");

			if (response.Error != null)
				throw new ServerException(response.Error.ErrorMessage);

			serverMetadata = response.Metadata;
			return response.Message;
		}

		public void SetConnector(IClientConnector c) {
			connector = c;
			processor = connector.CreateProcessor();
		}

		public void SetControlSystem(IControlSystem system) {
			controlSystem = system;
		}

		public void SetControlDatabase(IControlDatabase database) {
			controlDatabase = database;
		}

		public void Connect() {
			if (connector == null) {
				connector = CreateConnector();
				processor = connector.CreateProcessor();
			}

			remoteEndPoint = MakeRemoteEndPoint();

			var request = new ConnectRequest(connector.LocalEndPoint, remoteEndPoint) {
				DatabaseName = settings.Database, 
				Timeout = settings.QueryTimeout,
				IgnoreIdentifiersCase = settings.IgnoreIdentifiersCase,
				ParameterStyle = settings.ParameterStyle,
				AutoCommit = settings.AutoCommit
			};

			var response = SendMessage(request) as ConnectResponse;
			if (response == null)
				throw new ProtocolException("The returned message is invalid");

			if (!response.Opened)
				throw new ServerException("Was not able to open the connection on the server.");

			if (response.IsEncryted)
				connector.SetEncrypton(response.EncryptionData);

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
			var response = SendMessage(new AuthenticateRequest(settings.Schema, settings.UserName, settings.Password))
				as AuthenticateResponse;

			if (response == null)
				throw new InvalidOperationException();

			return response.Authenticated;
		}

		public IQueryResponse[] ExecuteQuery(SqlQuery query) {
			var response = SendMessage(new QueryExecuteRequest(query)) 
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
			var response = SendMessage(new BeginRequest(isolationLevel))
				as BeginResponse;

			if (response == null)
				throw new InvalidOperationException();

			return response.Id;
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

		public long CreateLrgeObject(ReferenceType type, long length) {
			var response = SendMessage(new LargeObjectCreateRequest(type, length))
				as LargeObjectCreateResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (response.ObjectLength != length || response.ReferenceType != type)
				throw new InvalidOperationException();

			return response.ObjectId;
		}

		public void DisposeLargeObject(long objId) {
			var response = SendMessage(new LargeObjectDisposeRequest(objId))
				as AcknowledgeResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (!response.State)
				throw new InvalidOperationException("Unable to dispose the large object on the server.");
		} 

		public IStreamableObjectChannel CreateLargeObjectChannel(long objId) {
			return connector.CreateObjectChannel(objId);
		}

		public ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			return connector.CreateTriggerChannel(triggerName, objectName, eventType);
		}

		public void Dispose() {
			
		}
	}
}