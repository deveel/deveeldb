// 
//  Copyright 2010-2016 Deveel
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
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Configuration;
using Deveel.Data.Protocol;
using Deveel.Data.Sql;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

using IsolationLevel = Deveel.Data.Transactions.IsolationLevel;

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

		public ConnectionClient(IClientConnector connector, DeveelDbConnectionStringBuilder settings) {
			if (connector == null)
				throw new ArgumentNullException("connector");

			Settings = settings;
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

		private static bool IsInMemory(string source) {
			return String.Equals(source, "In-Memory", StringComparison.OrdinalIgnoreCase) ||
			       String.Equals(source, "Memory", StringComparison.OrdinalIgnoreCase);
		}

		private static bool IsJournaled(string source) {
			return source.StartsWith("path=", StringComparison.OrdinalIgnoreCase) ||
			       source.StartsWith("directory=", StringComparison.OrdinalIgnoreCase) ||
			       source.Equals("journaled", StringComparison.OrdinalIgnoreCase);
		}

		private static bool IsSingleFile(string source) {
			return source.StartsWith("file=", StringComparison.OrdinalIgnoreCase) ||
			       source.Equals("file", StringComparison.OrdinalIgnoreCase);
		}

		private IDatabase CreateDatabase(IConfiguration configuration, IConfiguration dbConfig, string userName, string password, bool createIfNotExists) {
			var builder = new SystemBuilder(configuration);
			var system = builder.BuildSystem();

			var databaseName = dbConfig.GetString("database.name");

			IDatabase database;

			if (!system.DatabaseExists(databaseName)) {
				if (!createIfNotExists)
					throw new DeveelDbException(String.Format("The database '{0}' does not exist and it is not set to be created.",
						databaseName));
				database = system.CreateDatabase(dbConfig, userName, password);
			} else {
				database = system.OpenDatabase(dbConfig);
			}

			return database;
		}

		private IConfiguration CreateDbConfig(DeveelDbConnectionStringBuilder settings) {
			var config = new Configuration.Configuration();

			var databaseName = settings.Database;
			var dataSource = settings.DataSource;

			var schema = settings.Schema;
			if (String.IsNullOrEmpty(schema))
				schema = "APP";

			config.SetValue("database.name", databaseName);
			config.SetValue("database.defaultSchema", schema);

			if (IsInMemory(dataSource)) {
				config.SetValue("database.storeType", "memory");
			} else if (IsSingleFile(dataSource)) {
				var index = dataSource.IndexOf('=');
				var fileName = dataSource.Substring(index + 1);

				config.SetValue("database.storeType", "file");
				config.SetValue("database.path", fileName);
			} else if (IsJournaled(dataSource)) {
				var index = dataSource.IndexOf('=');

				var path = dataSource.Substring(index + 1);
				config.SetValue("database.storeType", "journaled");
				config.SetValue("database.path", path);
			}

			foreach (KeyValuePair<string, object> pair in settings) {
				var key = pair.Key;
				var value = pair.Value;

				// TODO: normalize the key and convert the value to set into the configuration
				config.SetValue(key, value);
			}

			return config;
		}

		private IClientConnector CreateConnector() {
			if (Connector != null)
				return Connector;

			// TODO: Extract system config from the connection string
			var sysConfig = new Configuration.Configuration();
			var dbConfig = CreateDbConfig(Settings);

			var userName = Settings.UserName;
			var password = Settings.Password;
			var createIfNotExists = Settings.BootOrCreate || Settings.Create;

			var database = CreateDatabase(sysConfig, dbConfig, userName, password, createIfNotExists);

			var handler = new SingleDatabaseHandler(database);
			return new EmbeddedClientConnector(new EmbeddedServerConnector(handler));
		}

		private IMessage SendMessage(IMessage message) {
			var envelope = Connector.CreateEnvelope(serverMetadata, message);
			var response = Processor.ProcessMessage(envelope);
			if (response == null)
				throw new InvalidOperationException("The processor returned no response.");

			if (response.Error != null)
				throw new DeveelDbServerException(response.Error.ErrorMessage, response.Error.ErrorClass, response.Error.ErrorCode);

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
				throw new DeveelDbServerException("Was not able to open the connection on the server.", -1, -1);

			if (response.IsEncryted)
				Connector.SetEncrypton(response.EncryptionData);

			IsClosed = false;
			ServerVersion = response.Version;
		}

		public void Disconnect() {
			try {
				if (Connector != null)
				{
					var response = SendMessage(new CloseRequest())
						as AcknowledgeResponse;

					if (response == null)
						throw new InvalidOperationException();

					if (!response.State)
						throw new DeveelDbServerException("Unable to close the connection on the server.", -1, -1);
				}
			} finally {
				IsClosed = true;
			}
		}

		public bool Authenticate() {
			var response = SendMessage(new AuthenticateRequest(Settings.Schema, Settings.UserName, Settings.Password))
				as AuthenticateResponse;

			if (response == null)
				throw new InvalidOperationException("No response from the server");

			return response.Authenticated;
		}

		public IQueryResponse[] ExecuteQuery(int commitId, SqlQuery query) {
			var response = SendMessage(new QueryExecuteRequest(commitId, query))
				as QueryExecuteResponse;

			if (response == null)
				throw new InvalidOperationException("No response from the server");

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
				throw new DeveelDbServerException(null, -1, -1);
		}

		public int BeginTransaction(System.Data.IsolationLevel isolationLevel) {
			var isolation = MapIsolationLevel(isolationLevel);
			return BeginTransaction(isolation);
		}

		private IsolationLevel MapIsolationLevel(System.Data.IsolationLevel isolationLevel) {
			if (isolationLevel == System.Data.IsolationLevel.Serializable)
				return IsolationLevel.Serializable;
			if (isolationLevel == System.Data.IsolationLevel.Snapshot)
				return IsolationLevel.Snapshot;
			if (isolationLevel == System.Data.IsolationLevel.ReadCommitted)
				return IsolationLevel.ReadCommitted;
			if (isolationLevel == System.Data.IsolationLevel.ReadUncommitted)
				return IsolationLevel.ReadUncommitted;

			throw new NotSupportedException(String.Format("Isolation Level '{0}' not supported by DeveelDB", isolationLevel));
		}

		public int BeginTransaction(IsolationLevel isolationLevel) {
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
				throw new DeveelDbServerException("Unable to commit the transaction on the server.", -1, -1);
		}

		public void RollbackTransaction(int transactionId) {
			var response = SendMessage(new RollbackRequest(transactionId))
				as AcknowledgeResponse;

			if (response == null)
				throw new InvalidOperationException();

			if (!response.State)
				throw new DeveelDbServerException("Unable to rollback the transaction on the server.", -1, -1);

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
