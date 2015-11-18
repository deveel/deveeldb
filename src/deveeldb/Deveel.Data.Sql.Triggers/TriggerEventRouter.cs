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

using Deveel.Data.Diagnostics;
using Deveel.Data.Services;

namespace Deveel.Data.Sql.Triggers {
	public class TriggerEventRouter : IEventRouter {
		public TriggerEventRouter(ISystemContext systemContext) {
			if (systemContext == null)
				throw new ArgumentNullException("systemContext");

			SystemContext = systemContext;
		}

		public ISystemContext SystemContext { get; private set; }

		public void RouteEvent(IEvent e) {
			if (!(e is TriggerEvent))
				return;

			var triggerEvent = (TriggerEvent) e;

			var listeners = SystemContext.ResolveAllServices<ITriggerListener>();
			foreach (var listener in listeners) {
				listener.OnTriggerEvent(triggerEvent);
			}
		}
	}
}
