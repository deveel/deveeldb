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
	public abstract class Event : IEvent {
		private IDictionary<string, object> metadata;
		private IEventSource source;

		protected Event() 
			: this(DateTimeOffset.UtcNow) {
		}

		protected Event(DateTimeOffset timeStamp) {
			TimeStamp = timeStamp;
		}

		private IDictionary<string, object> GenerateEventData() {
			var dictionary = new Dictionary<string, object>();
			GetEventData(dictionary);
			return dictionary;
		}

		protected virtual void GetEventData(Dictionary<string, object> data) {
		}

		public IEventSource EventSource {
			get {
				if (source == null)
					return null;

				return OnSourceGet(source);
			}
			set { source = OnSourceSet(value); }
		}

		public DateTimeOffset TimeStamp { get; private set; }

		protected virtual IEventSource OnSourceGet(IEventSource eventSource) {
			return eventSource;
		}

		protected virtual IEventSource OnSourceSet(IEventSource eventSource) {
			return eventSource;
		}

		IDictionary<string, object> IEvent.EventData {
			get {
				if (metadata == null)
					metadata = GenerateEventData();

				return metadata;
			}
		}
	}
}
