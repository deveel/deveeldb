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

using Deveel.Data.Configuration;

namespace Deveel.Data.Control {
	public sealed class LocalSystem : IControlSystem {
		private Dictionary<string, LocalDatabase> databases;

		private bool disposed;

		public LocalSystem(DbController controller) {
			Controller = controller;
		}

		~LocalSystem() {
			Dispose(false);
		}

		public DbController Controller { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (databases != null) {
					foreach (var database in databases.Values) {
						database.Dispose();
					}
				}

				databases = null;
			}

			disposed = true;
		}

		public IDbConfig Config {
			get { return Controller.Config; }
		}

		IControlDatabase IControlSystem.ControlDatabase(string database) {
			return ControlDatabase(database);
		}

		public LocalDatabase ControlDatabase(string database) {
			lock (this) {
				if (databases == null)
					databases = new Dictionary<string, LocalDatabase>();

				LocalDatabase controlDatabase;
				if (!databases.TryGetValue(database, out controlDatabase)) {
					controlDatabase = new LocalDatabase(this, database);
					databases[database] = controlDatabase;
				}

				return controlDatabase;
			}
		}
	}
}