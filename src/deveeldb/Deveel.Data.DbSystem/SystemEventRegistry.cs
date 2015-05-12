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
	public class SystemEventRegistry : IEventRegistry, IDisposable {
		private IEnumerable<IEventRouter> routers;

		public SystemEventRegistry(ISystemContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
		}

		public ISystemContext Context { get; private set; }

		private IEnumerable<IEventRouter> ResolveRouters() {
			return Context.ServiceProvider.ResolveAll<IEventRouter>();
		}

		public void RegisterEvent(IEvent e) {
			lock (this) {
				if (routers == null)
					routers = ResolveRouters();

				if (routers != null) {
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

			Context = null;
			routers = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
