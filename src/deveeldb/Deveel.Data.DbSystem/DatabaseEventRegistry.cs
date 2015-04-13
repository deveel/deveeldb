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
using System.Linq;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.DbSystem {
	public class DatabaseEventRegistry : IEventRegistry, IDisposable {
		private IEnumerable<IEventRouter> routers;

		public DatabaseEventRegistry(IDatabase database) {
			if (database == null)
				throw new ArgumentNullException("database");

			Database = database;
		}

		public IDatabase Database { get; private set; }

		protected virtual IDatabaseEvent CreateDatabaseEvent(IEvent e) {
			// TODO: revolve event converters 
			return null;
		}

		private IEnumerable<IEventRouter> ResolveRouters() {
			return Database.Context.SystemContext.ServiceProvider.ResolveAll<IEventRouter>();
		}

		public void RegisterEvent(IEvent e) {
			lock (this) {
				if (routers == null)
					routers = ResolveRouters();

				if (routers != null) {
					if (!(e is IDatabaseEvent))
						e = CreateDatabaseEvent(e);

					if (e == null)
						return;

					foreach (var router in routers) {
						try {
							router.RouteEvent(e);
						} catch (Exception) {
							// This is a final instance error that we have no way to 
							// catch, just ignore
						}
					}
				}
			}
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (routers != null) {
					foreach (var router in routers.OfType<IDisposable>()) {
						if (router != null)
							router.Dispose();
					}
				}
			}

			Database = null;
			routers = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#region DatabaseEvent

		class DatabaseEvent : IDatabaseEvent {
			public DatabaseEvent(string databaseName, string userName, byte eventType, int eventClass, int eventCode, string eventMessage) {
				DatabaseName = databaseName;
				UserName = userName;
				EventType = eventType;
				EventClass = eventClass;
				EventCode = eventCode;
				EventMessage = eventMessage;

			}

			public string DatabaseName { get; private set; }

			public string UserName { get; private set; }

			public byte EventType { get; private set; }

			public int EventClass { get; private set; }

			public int EventCode { get; private set; }

			public string EventMessage { get; private set; }

			public IDictionary<string, object> EventData { get; private set; }
		}

		#endregion
	}
}
