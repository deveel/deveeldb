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

using Deveel.Data.Services;

namespace Deveel.Data.Diagnostics {
	public static class SystemContextExtensions {
		public static void UseLogger(this ISystemContext context, Type loggerType) {
			if (loggerType == null)
				throw new ArgumentNullException("loggerType");
			if (!typeof(IEventLogger).IsAssignableFrom(loggerType))
				throw new ArgumentException(String.Format("Type '{0}' is not assignable from '{1}'", loggerType.AssemblyQualifiedName, typeof(IEventLogger)));

			context.ServiceProvider.Register<LogEventRouter>();
			context.ServiceProvider.Register(loggerType);
		}

		public static void UseLogger<TLogger>(this ISystemContext context) where TLogger : IEventLogger {
			context.UseLogger(typeof(TLogger));
		}

		public static void UseLogger(this ISystemContext context, IEventLogger logger) {
			if (logger == null)
				throw new ArgumentNullException("logger");

			context.ServiceProvider.Register(new LogEventRouter(context));
			context.ServiceProvider.Register(logger);
		}

#if !PCL
		public static void UseDefaultConsoleLogger(this ISystemContext context) {
			UseDefaultConsoleLogger(context, LogLevel.Warning);
		}

		public static void UseDefaultConsoleLogger(this ISystemContext context, LogLevel level) {
			context.UseLogger(new ConsoleEventLogger {Level = level});
		}
#endif
	}
}
