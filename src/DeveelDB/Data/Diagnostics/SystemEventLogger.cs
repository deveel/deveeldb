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

using Deveel.Data.Events;

namespace Deveel.Data.Diagnostics {
	public static class SystemEventLogger {
		public static void Attach(IDatabaseSystem system, ILogger logger, IEventTransformer transformer) {
			if (system == null)
				throw new ArgumentNullException(nameof(system));
			if (!(system is IEventHandler))
				throw new ArgumentException("The database system is not handling events");

			((IEventHandler)system).Consume(@event => {
				try {
					var entry = transformer?.Transform(@event);
					if (entry == null)
						throw new LogException("It was not possible to generate a log entry from an event");

					if (logger.IsInterestedIn(entry.Level))
						logger.LogAsync(entry).ConfigureAwait(false).GetAwaiter().GetResult();
				} catch(LogException) {
					throw;
				} catch (Exception ex) {
					throw new LogException("An error occurred while logging an event", ex);
				}

			});
		}
	}
}