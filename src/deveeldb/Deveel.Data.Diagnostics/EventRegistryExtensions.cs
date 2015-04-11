using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public static class EventRegistryExtensions {
		public static void Error(this IEventRegistry registry, IEventSource source, ErrorException ex) {
			var databaseName = String.Empty;
			var userName = String.Empty;

			if (source is IDatabaseEventSource) {
				databaseName = ((IDatabaseEventSource) source).DatabaseName;

				if (source is ISessionEventSource) {
					userName = ((ISessionEventSource) source).UserName;
				}
			}

			registry.RegisterEvent(ex.AsEvent(databaseName, userName));
		}

		public static void Error(this IEventRegistry registry, IEventSource source, Exception ex) {
			var databaseName = String.Empty;
			var userName = String.Empty;

			if (source is IDatabaseEventSource) {
				databaseName = ((IDatabaseEventSource) source).DatabaseName;

				if (source is ISessionEventSource) {
					userName = ((ISessionEventSource) source).UserName;
				}
			}

			var data = new Dictionary<string, object> {{"StackTrace", ex.StackTrace}, {"Source", ex.Source}};
			var errorEvent = new ErrorEvent(databaseName, userName, EventClasses.Runtime, -1, ErrorLevel.Error, ex.Message, data);
			registry.RegisterEvent(errorEvent);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, string message, string stackTrace, string errorSource) {
			Error(registry, source, errorClass, errorCode, ErrorLevel.Error, message, stackTrace, errorSource);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, ErrorLevel level) {
			Error(registry, source, errorClass, errorCode, level, null);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, ErrorLevel level, string message) {
			Error(registry, source, errorClass, errorCode, level, message, null, null);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, ErrorLevel level, string message, string stackTrace, string errorSource) {
			var databaseName = String.Empty;
			var userName = String.Empty;

			if (source is IDatabaseEventSource) {
				databaseName = ((IDatabaseEventSource)source).DatabaseName;

				if (source is ISessionEventSource) {
					userName = ((ISessionEventSource)source).UserName;
				}
			}

			var data = new Dictionary<string, object> {{"StackTrace", stackTrace}, {"Source", errorSource}};
			registry.RegisterEvent(new ErrorEvent(databaseName, userName, errorClass, errorCode, level, message, data));
		}
	}
}
