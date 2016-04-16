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
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	public sealed class EventRegistry : ThreadedQueue<IEvent>, IEventRegistry {
		private bool disposed;

		private readonly int threadCount;

		public EventRegistry(IContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;

			var config = context.ResolveService<IConfiguration>();
			threadCount = config.GetInt32("system.events.threadCount", 2);
		}

		~EventRegistry() {
			Dispose(false);
		}

		public IContext Context { get; private set; }

		public override int ThreadCount {
			get { return threadCount; }
		}

		public void RegisterEvent(IEvent @event) {
			Enqueue(@event);
		}

		private IEnumerable<IEventRouter> GetAllRouters() {
			var list = new List<IEventRouter>();
			var context = Context;
			while (context != null) {
				var routers = context.ResolveAllServices<IEventRouter>();
				list.AddRange(routers);
				context = context.Parent;
			}

			return list;
		} 

		protected override void Consume(IEvent message) {
			var routers = GetAllRouters();

			foreach (var router in routers) {
				try {
					if (router.CanRoute(message))
						router.RouteEvent(message);
				} catch (Exception ex) {
					Enqueue(new ErrorEvent(ex, -1, ErrorLevel.Critical));
				}
			}
		}

		protected override void Dispose(bool disposing) {
			if (!disposed) {
				Context = null;
				disposed = true;
			}

			base.Dispose(disposing);
		}
	}
}
