using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Diagnostics {
	public static class LoggerContextExtensions {
		public static void UseLogger(this ISystemContext context, Type loggerType) {
			context.ServiceProvider.Register<LogEventRouter>();
			context.ServiceProvider.Register(loggerType);
		}

		public static void UseDefaultLogger(this ISystemContext context) {
			
		}
	}
}
