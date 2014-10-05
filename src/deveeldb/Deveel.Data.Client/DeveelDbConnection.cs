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

using Deveel.Data.Control;
using Deveel.Data.Protocol;
using Deveel.Data.Routines;

using DataTable = System.Data.DataTable;

namespace Deveel.Data.Client {
	public sealed class DeveelDbConnection : DbConnection {
		private DeveelDbConnectionStringBuilder connectionString;

		private ConnectionState state;
		private ConnectionState oldState;
		private readonly object stateLock = new object();

		private string serverVersion;
		private string dataSource;

		private IDictionary<string, DeveelDbDataReader> openReaders;

		private DatabaseMetadata metadata;

		private bool ownsConnector = true;

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

		internal DeveelDbConnection(string connectionString, IClientConnector connector)
			: this(connectionString) {
			Client.SetConnector(connector);
		}

		internal DeveelDbConnection(string connectionString, IControlSystem controlSystem)
			: this(connectionString) {
			Client.SetControlSystem(controlSystem);
		}

		internal DeveelDbConnection(string connectionString, IControlDatabase controlDatabase)
			: this(connectionString, controlDatabase.System) {
			Client.SetControlDatabase(controlDatabase);
		}

		private void Init() {
			RowCache = new RowCache(this);
			metadata = new DatabaseMetadata(this);
			ChangeState(ConnectionState.Closed);
			Client = new ConnectionClient(Settings);
		}

		internal DeveelDbTransaction CurrentTransaction { get; private set; }

		internal ConnectionClient Client { get; private set; }
		
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
				try {
					var id = Client.BeginTransaction(isolationLevel);
					CurrentTransaction = new DeveelDbTransaction(this, id);
					return CurrentTransaction;
				} catch (Exception ex) {
					throw new DeveelDbException(ex.Message, -1, -1);
				}
			}
		}

		public new DeveelDbTransaction BeginTransaction() {
			return BeginTransaction(IsolationLevel.Serializable);
		}

		internal void CommitTransaction(int id) {
			lock (this) {
				try {
					Client.CommitTransaction(id);
				} catch (Exception ex) {
					throw new DeveelDbException(ex.Message, -1, -1);
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
					Client.RollbackTransaction(id);
				} catch (Exception ex) {
					throw new DeveelDbException(ex.Message, -1, -1);
				} finally {
					if (CurrentTransaction != null &&
						CurrentTransaction.Id == id)
					CurrentTransaction = null;
				}
			}
		}

		public override void Close() {
			lock (this) {
				try {
					if (State == ConnectionState.Closed)
						return;

					if (openReaders != null) {
						foreach (var reader in openReaders.Values) {
							reader.Close();
						}

						openReaders.Clear();
						openReaders = null;
					}


					if (CurrentTransaction != null) {
						CurrentTransaction.Rollback();
						CurrentTransaction = null;
					}

					Client.Disconnect();
				} catch (Exception ex) {
					throw new DeveelDbException(ex.Message, -1, -1);
				} finally {
					ChangeState(ConnectionState.Closed);
				}
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (State != ConnectionState.Closed)
					Close();

				if (ownsConnector) {
					Client.Dispose();
				}

				Client = null;
			}

			base.Dispose(disposing);
		}

		internal void DisposeResult(int resultId) {
			lock (this) {
				try {
					Client.DisposeResult(resultId);
				} catch (Exception ex) {
					throw new DeveelDbException(ex.Message, -1, -1);
				}
			}
		}

		public override void ChangeDatabase(string databaseName) {
			lock (this) {
				if (String.IsNullOrEmpty(databaseName))
					throw new ArgumentNullException("databaseName");

				if (String.Equals(Settings.Database, databaseName))
					return;

				// TODO: maybe it's best to have a dedicated message?

				Close();

				Settings.Database = databaseName;

				Open();
			}
		}

		public override void Open() {
			lock (this) {
				if (State != ConnectionState.Closed)
					return;

				try {
					ChangeState(ConnectionState.Connecting);

					Client.Connect();
					Client.Authenticate();

					serverVersion = Client.ServerVersion;
				} catch (Exception ex) {
					ChangeState(ConnectionState.Broken);

					// TODO: throw a specialized exception
					throw;
				}

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
			try {
				return Client.ExecuteQuery(query);
			} catch (Exception ex) {
				throw new DeveelDbException(ex.Message, -1, -1);
			}
		}

		internal QueryResultPart RequestResultPart(int resultId, int rowIndex, int rowCount) {
			try {
				return Client.GetResultPart(resultId, rowIndex, rowCount);
			} catch (Exception ex) {
				throw new DeveelDbException(ex.Message, -1, -1);
			}
		}

		internal StreamableObject CreateStreamableObject(ReferenceType referenceType, long length) {
			try {
				var objId = Client.CreateLrgeObject(referenceType, length);
				return new StreamableObject(referenceType, length, objId);
			} catch (Exception ex) {
				throw new DeveelDbException(ex.Message, -1, -1);
			}
		}

		internal IStreamableObjectChannel OpenObjectChannel(long objId) {
			try {
				return Client.CreateLargeObjectChannel(objId);
			} catch (Exception ex) {
				throw new DeveelDbException(ex.Message, -1, -1);
			}
		}

		internal void DisposeObject(long objId) {
			try {
				Client.DisposeLargeObject(objId);
			} catch (Exception ex) {
				throw new DeveelDbException(ex.Message, -1, -1);
			}
		}

		internal ITriggerChannel OpenTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			try {
				return Client.CreateTriggerChannel(triggerName, objectName, eventType);
			} catch (Exception ex) {
				throw new DeveelDbException(ex.Message, -1, -1);
			}
		}

		public static DeveelDbConnection Connect(IClientConnector connector) {
			return Connect(new DeveelDbConnectionStringBuilder(), connector);
		}

		public static DeveelDbConnection Connect(string connectionString, IClientConnector connector) {
			if (connectionString == null) 
				throw new ArgumentNullException("connectionString");

			return Connect(new DeveelDbConnectionStringBuilder(connectionString), connector);
		}

		public static DeveelDbConnection Connect(DeveelDbConnectionStringBuilder connectionString, IClientConnector connector) {
			if (connectionString == null) 
				throw new ArgumentNullException("connectionString");
			if (connector == null)
				throw new ArgumentNullException("connector");

			var connection = new DeveelDbConnection(connectionString.ToString(), connector);
			connection.ownsConnector = false;
			connection.Open();

			if (connection.State != ConnectionState.Open)
				throw new InvalidOperationException("Unable to connect.");

			return connection;
		}
	}
}