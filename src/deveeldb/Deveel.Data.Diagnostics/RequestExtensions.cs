using System;

namespace Deveel.Data.Diagnostics {
	public static class RequestExtensions {
		public static void Route<TEvent>(this IRequest request, Action<TEvent> router) 
			where TEvent : class, IEvent {
			request.Route(router, null);
		}

		public static void Route<TEvent>(this IRequest request, Action<TEvent> router, Func<TEvent, bool> condition) 
			where TEvent : class, IEvent {
			request.Context.Route(router, condition);
		}
	}
}
