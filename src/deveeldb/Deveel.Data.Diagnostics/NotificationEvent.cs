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
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public class NotificationEvent : IEvent {
		public NotificationEvent(NotificationLevel level, int eventClass, int eventCode, string eventMessage) {
			Level = level;
			EventClass = eventClass;
			EventCode = eventCode;
			EventMessage = eventMessage;
		}

		byte IEvent.EventType {
			get { return (byte) EventType.Notification; }
		}

		public int EventClass { get; private set; }

		public int EventCode { get; private set; }

		public NotificationLevel Level { get; private set; }

		public string EventMessage { get; private set; }

		IDictionary<string, object> IEvent.EventData {
			get {
				var eventData = new Dictionary<string, object> {
					{"EventClass", EventClass},
					{"EventCode", EventCode},
					{"EventMessage", EventMessage},
					{"Level", Level}
				};

				FillEventData(eventData);

				return eventData;
			}
		}

		protected virtual void FillEventData(IDictionary<string, object> eventData) {
		}
	}
}
