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

namespace Deveel.Data.Diagnostics {
	[Serializable]
	public sealed class EventLog {
		public EventLog(IEvent @event, LogLevel level) 
			: this(@event, level, DateTime.UtcNow) {
		}

		public EventLog(IEvent @event, LogLevel level, DateTime date) {
			if (@event == null)
				throw new ArgumentNullException("event");

			Event = @event;
			Level = level;
			Date = date;
		}

		public LogLevel Level { get; private set; }

		public IEvent Event { get; private set; }

		public DateTime Date { get; private set; }
	}
}
