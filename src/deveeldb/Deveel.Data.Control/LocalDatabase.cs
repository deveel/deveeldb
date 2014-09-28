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
using System.Globalization;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;

namespace Deveel.Data.Control {
	public sealed class LocalDatabase : IControlDatabase {
		private readonly string databaseName;

		private DbSystem dbsys;
		private int openConnections;

		public LocalDatabase(LocalSystem system, string databaseName) {
			System = system;
			this.databaseName = databaseName;
		}

		public void Dispose() {
		}

		public bool IsBooted { get; private set; }

		IControlSystem IControlDatabase.System {
			get { return System; }
		}

		public LocalSystem System { get; private set; }

		public IConnector Create(IDbConfig config, string userName, string password) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			if (IsBooted)
				throw new DatabaseException("The database was already booted.");

			// Create the DbSystem and bind it to a IDatabaseInterface.
			dbsys = System.Controller.CreateDatabase(config, databaseName, userName, password);
			var connector = new LocalEmbeddedServerConnector(this);

			IsBooted = true;
			++openConnections;

			return connector;
		}

		public IConnector Boot(IDbConfig config) {
			if (IsBooted)
				throw new DatabaseException("Database was booted more than once.");

			// Start the DbSystem and bind it to a IDatabaseInterface.
			if (System.Controller.IsInitialized(databaseName))
				dbsys = System.Controller.ConnectToDatabase(config, databaseName);
			else
				dbsys = System.Controller.StartDatabase(config, databaseName);

			var connector = new LocalEmbeddedServerConnector(this);

			IsBooted = true;
			++openConnections;

			return connector;
		}

		public IConnector Connect(IDbConfig config) {
			if (!IsBooted)
				throw new DatabaseException("The database is not started.");

			var connector = new LocalEmbeddedServerConnector(this);

			++openConnections;

			return connector;
		}

		public bool CheckExists(IDbConfig config) {
			if (IsBooted)
				return true;

			return System.Controller.DatabaseExists(config, databaseName);
		}

		#region SingleDatabaseHandler

		class SingleDatabaseHandler : IDatabaseHandler {
			private readonly IDatabase database;

			public SingleDatabaseHandler(IDatabase database) {
				this.database = database;
			}

			public IDatabase GetDatabase(string name) {
				if (!String.IsNullOrEmpty(name) && 
					!String.Equals(database.Name, name))
					return null;

				return database;
			}
		}

		#endregion

		#region LocalConnector

		class LocalEmbeddedServerConnector : EmbeddedServerConnector {
			private readonly LocalDatabase controlDatabase;

			public LocalEmbeddedServerConnector(LocalDatabase database) 
				: base(new SingleDatabaseHandler(database.dbsys.Database)) {
				controlDatabase = database;
			}

			protected override void Dispose(bool disposing) {
				bool doDispose = false;

				if (disposing) {
					controlDatabase.openConnections--;

					if (controlDatabase.openConnections <= 0) {
						// When the local database connector is disposed, we must shut down
						// the database system.
						controlDatabase.dbsys.Close();
						controlDatabase.IsBooted = false;
						controlDatabase.dbsys = null;
						doDispose = true;
					}
				}

				if (doDispose)
					base.Dispose(true);
			}
		}

		#endregion
	}
}