using System;

using Deveel.Data.Services;

namespace Deveel.Data.Diagnostics {
	public static class ContextExtensions {
		public static void RegisterEvent(this IContext context, IEvent @event) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IEventScope) {
					var scope = (IEventScope) currentContext;
					scope.EventRegistry.RegisterEvent(@event);
				}

				currentContext = currentContext.Parent;
			}
		}
	}
}
