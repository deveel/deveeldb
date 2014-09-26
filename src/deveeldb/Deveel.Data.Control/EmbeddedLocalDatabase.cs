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

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;

namespace Deveel.Data.Control {
	public sealed class EmbeddedLocalDatabase : ILocalDatabase {
		private readonly DbController controller;
		private readonly string databaseName;

		private Control.DbSystem dbsys;
		private int connectId;
		private int openConnections;

		public EmbeddedLocalDatabase(DbController controller, string databaseName) {
			this.controller = controller;
			this.databaseName = databaseName;
		}

		public void Dispose() {
		}

		public bool IsBooted { get; private set; }

		public IConnector Create(IDbConfig config, string userName, string password) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			if (IsBooted)
				throw new DatabaseException("The database was already booted.");

			// Create the DbSystem and bind it to a IDatabaseInterface.
			dbsys = controller.CreateDatabase(config, databaseName, userName, password);
			var connector = new EmbeddedServerConnector(dbsys);

			IsBooted = true;
			++openConnections;

			return connector;
		}

		public IConnector Boot(IDbConfig config) {
			throw new NotImplementedException();
		}

		public IConnector Connect(IDbConfig config) {
			throw new NotImplementedException();
		}

		public bool CheckExists(IDbConfig config) {
			throw new NotImplementedException();
		}
	}
}