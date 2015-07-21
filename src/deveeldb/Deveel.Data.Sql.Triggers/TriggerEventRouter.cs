using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Diagnostics;

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

			var listeners = SystemContext.ServiceProvider.ResolveAll<ITriggerListener>();
			foreach (var listener in listeners) {
				listener.OnTriggerEvent(triggerEvent);
			}
		}
	}
}
