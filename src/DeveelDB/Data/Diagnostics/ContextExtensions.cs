using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Deveel.Data.Diagnostics {
	public static class ContextExtensions {
		public static async Task LogAsync(this IContext context, LogEntry entry) {
			if (entry == null)
				return;

			var parent = context;

			while (parent != null) {
				if (parent is ILoggingContext) {
					var logger = ((ILoggingContext) parent).Logger;
					if (logger.IsInterestedIn(entry.Level))
						await logger.LogAsync(entry);
				}

				parent = parent.ParentContext;
			}
		}

		public static Task LogAsync(this IContext context, object source, string message, LogLevel level,
			IDictionary<string, object> data = null) {
			return context.LogAsync(new LogEntry(source, message, level) {
				Data = data
			});
		}

		public static Task DebugAsync(this IContext context, string message, IDictionary<string, object> data = null) {
			return DebugAsync(context, context, message, data);
		}

		public static Task DebugAsync(this IContext context, object source, string message, IDictionary<string, object> data = null)
			=> context.LogAsync(source, message, LogLevel.Debug, data);
	}
}