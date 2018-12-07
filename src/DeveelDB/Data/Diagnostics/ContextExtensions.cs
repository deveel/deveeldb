// 
//  Copyright 2010-2018 Deveel
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