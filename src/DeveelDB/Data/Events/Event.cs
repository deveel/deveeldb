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

namespace Deveel.Data.Events {
	public class Event : IEvent {
		public Event(IEventSource source, Guid id) 
			: this(source, id, DateTimeOffset.UtcNow) {
		}

		public Event(IEventSource source)
			: this(source, Guid.NewGuid()) {
		}

		public Event(IEventSource source, Guid id, DateTimeOffset timeStamp) {
			EventSource = source ?? throw new ArgumentNullException(nameof(source));
			EventId = id;
			TimeStamp = timeStamp;

			Data = new Dictionary<string, object>();
		}

		public IEventSource EventSource { get; }

		public Guid EventId { get; }

		public DateTimeOffset TimeStamp { get; }

		public IDictionary<string, object> Data { get; set; }

		IDictionary<string, object> IEvent.EventData {
			get {
				var data = new Dictionary<string, object>(Data);
				data["id"] = EventId.ToString("N");
				data["timeStamp"] = TimeStamp.ToUniversalTime().ToUnixTimeMilliseconds();

				var metadata = EventSource.Metadata;

				foreach (var pair in metadata) {
					data[pair.Key] = pair.Value;
				}

				GetEventData(data);
				return data;
			}
		}

		protected virtual void GetEventData(IDictionary<string, object> data) {

		}
	}
}
