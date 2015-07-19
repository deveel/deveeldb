using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Diagnostics {
	public static class QueryContextEventExtensions {
		public static void RegisterEvent(this IQueryContext context, IEvent @event) {
			context.DatabaseContext.SystemContext.EventRegistry.RegisterEvent(@event);
		}

		public static void RegisterError(this IQueryContext context, string message, Exception error) {
			var errorEx = new ErrorException(EventClasses.Runtime, Int32.MinValue, message, error);
			var errorEvent = errorEx.AsEvent(context.Session);
			context.RegisterEvent(errorEvent);
		}

		public static void RegisterError(this IQueryContext context, ErrorException error) {
			var errorEvent = error.AsEvent(context.Session);
			context.RegisterEvent(errorEvent);
		}

		public static void RegisterError(this IQueryContext context, int eventClass, int errorCode, string message) {
			context.RegisterError(new ErrorException(eventClass, errorCode, message));
		}
	}
}
