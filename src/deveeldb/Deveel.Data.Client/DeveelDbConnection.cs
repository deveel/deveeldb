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
using System.Data;
using System.Data.Common;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;
using Deveel.Data.Routines;

using DataTable = System.Data.DataTable;

namespace Deveel.Data.Client {
	public sealed class DeveelDbConnection : DbConnection {
		private ConnectionProcessor processor;
		private IControlDatabase controlDatabase;
		private IConnector connector;
		private DeveelDbConnectionStringBuilder connectionString;

		private ConnectionState state;
		private ConnectionState oldState;
		private readonly object stateLock = new object();

		private string serverVersion;
		private string dataSource;

		private IDictionary<string, DeveelDbDataReader> openReaders;

		private int transactionId = -1;

		private DatabaseMetadata metadata;

		public DeveelDbConnection() 
			: this((DeveelDbConnectionStringBuilder)null) {
		}

		public DeveelDbConnection(DeveelDbConnectionStringBuilder connectionString) {
			if (connectionString != null)
				Settings = connectionString;

			Init();
		}

		public DeveelDbConnection(string connectionString)
			: this(new DeveelDbConnectionStringBuilder(connectionString)) {
		}

		internal DeveelDbConnection(string connectionString, IConnector connector)
			: this(connectionString) {
			this.connector = connector;
		}

		internal DeveelDbConnection(string connectionString, IControlDatabase controlDatabase)
			: this(connectionString) {
			this.controlDatabase = controlDatabase;
		}

		private void Init() {
			RowCache = new RowCache(this);
			metadata = new DatabaseMetadata(this);
			ChangeState(ConnectionState.Closed);
		}

		private IDbConfig CreateDbConfig(IControlSystem controlSystem) {
			var dbConfig = new DbConfig(controlSystem.Config);
			dbConfig.DatabaseName(Settings.Database);
			dbConfig.IgnoreIdentifierCase(Settings.IgnoreIdentifiersCase);

			// TODO: More mappings?

			return dbConfig;
		}

		private IConnector CreateLocalDatabaseConnector() {
			var dbConfig = CreateDbConfig(controlDatabase.System);
			if (controlDatabase.CheckExists(dbConfig)) {
				if (controlDatabase.IsBooted) {
					return controlDatabase.Connect(dbConfig);
				}
				if (Settings.BootOrCreate) {
					return controlDatabase.Boot(dbConfig);
				}
			} else if (Settings.BootOrCreate) {
				return controlDatabase.Create(dbConfig, Settings.UserName, Settings.Password);
			}

			throw new InvalidOperationException();
		}

		private IConnector CreateConnector() {
			if (connector != null)
				return connector;

			if (IsInMemory(Settings.DataSource) &&
				controlDatabase == null) {
				// TODO: handle the case the connection string does not specify a database name
				var databaseName = Settings.Database;
				if (String.IsNullOrEmpty(databaseName))
					throw new InvalidOperationException();

				var config = DbConfig.Default;
				config.StorageSystem(ConfigDefaultValues.HeapStorageSystem);
				var controller = DbController.Create(config);

				var localSystem = new LocalSystem(controller);
				controlDatabase = localSystem.ControlDatabase(databaseName);
			} else if (IsInFileSystem(Settings) &&
			           controlDatabase == null) {
				// TODO: handle the case the connection string does not specify a database name
				var databaseName = Settings.Database;
				if (String.IsNullOrEmpty(databaseName))
					throw new InvalidOperationException();

				var config = DbConfig.Default;
				config.StorageSystem(ConfigDefaultValues.FileStorageSystem);

				var basePath = Settings.DataSource;
				if (String.Equals(basePath, "local", StringComparison.OrdinalIgnoreCase))
					basePath = Settings.Path;
				if (String.IsNullOrEmpty(basePath))
					basePath = Environment.CurrentDirectory;

				config.BasePath(basePath);

				// This finds the datbase in the file-system if exists, otherwise it will create
				var controller = DbController.Create(config);

				var localSystem = new LocalSystem(controller);
				controlDatabase = localSystem.ControlDatabase(databaseName);
			} else if (controlDatabase == null) {
				return CreateNetworkConnector();
			}

			if (controlDatabase != null)
				return CreateLocalDatabaseConnector();

			throw new InvalidOperationException("Unable to create a connector to the database");
		}

		private IConnector CreateNetworkConnector() {
			throw new NotImplementedException();
		}

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

		internal DeveelDbTransaction CurrentTransaction { get; private set; }

		public override DataTable GetSchema(string collectionName) {
			return metadata.GetSchema(collectionName, new[]{Settings.Schema});
		}

		public override DataTable GetSchema() {
			return metadata.GetSchemata(new [] {Settings.Schema});
		}

		public override DataTable GetSchema(string collectionName, string[] restrictionValues) {
			return metadata.GetSchema(collectionName, restrictionValues);
		}

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
			return BeginTransaction(isolationLevel);
		}

		public new DeveelDbTransaction BeginTransaction(IsolationLevel isolationLevel) {
			if (CurrentTransaction != null)
				throw new InvalidOperationException("A transaction is already open");

			if (isolationLevel != IsolationLevel.Serializable &&
				isolationLevel != IsolationLevel.Unspecified)
				throw new NotSupportedException();

			lock (this) {
				processor.Begin();
				CurrentTransaction = new DeveelDbTransaction(this, transactionId++);
				return CurrentTransaction;
			}
		}

		public new DeveelDbTransaction BeginTransaction() {
			return BeginTransaction(IsolationLevel.Serializable);
		}

		internal void CommitTransaction(int id) {
			lock (this) {
				try {
					processor.Commit();
				} catch (Exception) {

					throw;
				} finally {
					if (CurrentTransaction != null &&
						CurrentTransaction.Id == id)
					CurrentTransaction = null;
				}
			}
		}

		internal void RollbackTransaction(int id) {
			lock (this) {
				try {
					processor.Rollback();
				} catch (Exception) {

					throw;
				} finally {
					if (CurrentTransaction != null &&
						CurrentTransaction.Id == id)
					CurrentTransaction = null;
				}
			}
		}

		public override void Close() {
			lock (this) {
				
			}
		}

		internal void DisposeResult(int resultId) {
			lock (this) {
				processor.DisposeResult(resultId);
			}
		}

		public override void ChangeDatabase(string databaseName) {
			lock (this) {
				if (openReaders != null) {
					foreach (var reader in openReaders.Values) {
						reader.Close();
					}

					openReaders.Clear();
					openReaders = null;
				}

				// TODO: Close the connection, change the database and reconnect ...

				throw new NotImplementedException();
			}
		}

		public override void Open() {
			lock (this) {
				if (State != ConnectionState.Closed)
					return;

				if (connector == null)
					connector = CreateConnector();

				if (processor == null)
					processor = new ConnectionProcessor(Settings, connector);

				string dbVersion;

				try {
					ChangeState(ConnectionState.Connecting);

					processor.Connect(out dbVersion);
					processor.Authenticate(Settings.Schema, Settings.UserName, Settings.Password);
				} catch (Exception ex) {
					ChangeState(ConnectionState.Broken);

					// TODO: throw a specialized exception
					throw;
				}

				serverVersion = dbVersion;

				ChangeState(ConnectionState.Open);
			}
		}

		public override string ConnectionString {
			get { return Settings.ToString(); }
			set { Settings = new DeveelDbConnectionStringBuilder(value); }
		}

		internal DeveelDbConnectionStringBuilder Settings {
			get { return connectionString; }
			set {
				AssertClosed();
				connectionString = value;
				Init();
			}
		}

		internal RowCache RowCache { get; private set; }

		public override int ConnectionTimeout {
			get { return Settings.QueryTimeout; }
		}

		public override string Database {
			get { return Settings.Database; }
		}

		public override ConnectionState State {
			get {
				lock (stateLock) {
					return state;
				}
			}
		}

		private void AssertClosed() {
			if (state != ConnectionState.Closed)
				throw new InvalidOperationException("The connection is not closed.");
		}

		private void AssertFetching() {
			if (state != ConnectionState.Fetching)
				throw new InvalidOperationException("The connection is not fetching data.");
		}

		internal void ChangeState(ConnectionState newState) {
			lock (stateLock) {
				if (state != newState)
					OnStateChange(new StateChangeEventArgs(state, newState));

				oldState = state;
				state = newState;
			}
		}

		internal void EndState() {
			ChangeState(oldState);
		}

		public override string DataSource {
			get {
				if (String.IsNullOrEmpty(dataSource)) {
					if (String.IsNullOrEmpty(connectionString.DataSource) &&
					    !String.IsNullOrEmpty(connectionString.Host)) {
						dataSource = String.Format("{0}:{1}", connectionString.Host, connectionString.Port);
					} else {
						dataSource = connectionString.DataSource;
					}
				}

				return dataSource;
			}
		}

		public override string ServerVersion {
			get { return serverVersion; }
		}

		protected override DbCommand CreateDbCommand() {
			return CreateCommand();
		}

		public new DeveelDbCommand CreateCommand() {
			return new DeveelDbCommand(this);
		}

		public DeveelDbCommand CreateCommand(string commandText) {
			return new DeveelDbCommand(this, commandText);
		}

		internal IQueryResponse[] ExecuteQuery(SqlQuery query) {
			if (processor == null)
				throw new InvalidOperationException();

			return processor.ExecuteQuery(query);
		}

		internal QueryResultPart RequestResultPart(int resultId, int rowIndex, int rowCount) {
			if (processor == null)
				throw new InvalidOperationException();

			return processor.RequestResultPart(resultId, rowIndex, rowCount);
		}

		internal StreamableObject CreateStreamableObject(ReferenceType referenceType, long length, ObjectPersistenceType persistence) {
			var objId = processor.CreateObject(referenceType, length, persistence);
			return new StreamableObject(referenceType, length, objId);
		}

		internal IStreamableObjectChannel OpenObjectChannel(long objId, ObjectPersistenceType persistence) {
			if (connector == null)
				throw new InvalidOperationException();

			return connector.CreateObjectChannel(objId, persistence);
		}

		internal void DisposeObject(long objId) {
			processor.DisposeObject(objId);
		}

		internal ITriggerChannel OpenTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			return connector.CreateTriggerChannel(triggerName, objectName, eventType);
		}

		#region ConnectionProcessor

		class ConnectionProcessor {
			private readonly DeveelDbConnectionStringBuilder settings;
			private readonly IConnector connector;
			private readonly IMessageProcessor processor;

			public ConnectionProcessor(DeveelDbConnectionStringBuilder settings, IConnector connector) {
				this.settings = settings;
				this.connector = connector;
				processor = connector.CreateProcessor();
			}

			private IMessageEnvelope CreateEnvelope(IMessage message) {
				return connector.CreateEnvelope(null, message);
			}

			private IMessage Process(IMessage message) {
				var envelope = CreateEnvelope(message);
				var response = processor.ProcessMessage(envelope);

				if (response.Error != null)
					throw new ServerException(response.Error.ErrorMessage, response.Error.ErrorClass, response.Error.ErrorCode);

				return response.Message;
			}

			public void Authenticate(string defaultSchema, string username, string password) {
				var request = new AuthenticateRequest(defaultSchema, username, password);
				var response = (AuthenticateResponse) Process(request);
				if (!response.Authenticated)
					throw new DatabaseException("Unable to authenticate.");
			}

			public void Connect(out string databaseVesion) {
				var request = new ConnectRequest(connector.LocalEndPoint) {
					AutoCommit = settings.AutoCommit,
					IgnoreIdentifiersCase = settings.IgnoreIdentifiersCase,
					ParameterStyle = settings.ParameterStyle
				};

				var response = (ConnectResponse) Process(request);
				if (!response.Opened)
					throw new DatabaseException("Unable to open the connection");

				if (response.IsEncryted) {
					if (response.EncryptionData == null)
						throw new DatabaseException("The connection wis encrypted, but no encryption data were received.");

					if (connector is IClientConnector)
						((IClientConnector)connector).SetEncrypton(response.EncryptionData);
				}

				 databaseVesion = response.Version;
			}

			public void Disconnect() {
				var request = new CloseCommand();
				var response = (AcknowledgeResponse) Process(request);
				if (!response.State)
					throw new DatabaseException("Could not close the connection successfully");
			}

			public IQueryResponse[] ExecuteQuery(SqlQuery query) {
				var response = (QueryExecuteResponse) Process(new QueryExecuteRequest(query));
				return response.QueryResponse;
			}

			public QueryResultPart RequestResultPart(int resultId, int rowIndex, int rowCount) {
				var request = new QueryResultPartRequest(resultId, rowIndex, rowCount);
				var response = (QueryResultPartResponse) Process(request);
				return response.Part;
			}

			public void DisposeResult(int id) {
				var request = new DisposeResultRequest(id);
				var response = (AcknowledgeResponse) Process(request);
				if (!response.State)
					throw new InvalidOperationException("Could not dispose the result on the server.");
			}

			public long CreateObject(ReferenceType referenceType, long length, ObjectPersistenceType persistence) {
				var request = new StreamableObjectCreateRequest(referenceType, length, persistence);
				var response = (StreamableObjectCreateResponse) Process(request);
				return response.ObjectId;
			}

			public void DisposeObject(long id) {
				throw new NotImplementedException();
			}

			public void Begin() {
				var response = (AcknowledgeResponse) Process(new BeginRequest());
				if (!response.State)
					throw new InvalidOperationException("Unable to begin the transaction.");
			}

			public void Commit() {
				var response = (AcknowledgeResponse) Process(new CommitRequest());
				if (!response.State)
					throw new InvalidOperationException("Was not able to commit changes to the server.");
			}

			public void Rollback() {
				var response = (AcknowledgeResponse) Process(new RollbackRequest());
				if (!response.State)
					throw new InvalidOperationException("Could not rollback on the server.");
			}
		}

		#endregion
	}
}