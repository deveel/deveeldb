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

namespace Deveel.Data.Control {
	public sealed class EmbeddedLocalSystem : ILocalSystem {
		private readonly DbController controller;
		private Dictionary<string, EmbeddedLocalDatabase> databases;

		private bool disposed;

		public EmbeddedLocalSystem(DbController controller) {
			this.controller = controller;
		}

		~EmbeddedLocalSystem() {
			Dispose(false);
		}

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

		ILocalDatabase ILocalSystem.ControlDatabase(string database) {
			return ControlDatabase(database);
		}

		public EmbeddedLocalDatabase ControlDatabase(string database) {
			lock (this) {
				if (databases == null)
					databases = new Dictionary<string, EmbeddedLocalDatabase>();

				EmbeddedLocalDatabase localDatabase;
				if (!databases.TryGetValue(database, out localDatabase)) {
					localDatabase = new EmbeddedLocalDatabase(controller, database);
					databases[database] = localDatabase;
				}

				return localDatabase;
			}
		}
	}
}