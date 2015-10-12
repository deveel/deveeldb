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

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerEvent : IEvent {
		internal TriggerEvent(IEventSource source, ObjectName triggerName, ObjectName sourceName, TriggerEventType eventType, RowId oldRowId, Row newRow) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (sourceName == null)
				throw new ArgumentNullException("sourceName");

			Source = source;
			TriggerName = triggerName;
			SourceName = sourceName;
			TriggerEventType = eventType;
			OldRowId = oldRowId;
			NewRow = newRow;
		}

		byte IEvent.EventType {
			get { return (byte) EventType.Notification; }
		}

		int IEvent.EventClass {
			get { return EventClasses.SqlModel; }
		}

		int IEvent.EventCode {
			get {
				// TODO:
				return -1;
			}
		}

		IEventSource IEvent.EventSource {
			get { return Source; }
		}

		private IEventSource Source { get; set; }

		public ObjectName TriggerName { get; private set; }

		public ObjectName SourceName { get; set; }

		public TriggerEventType TriggerEventType { get; private set; }

		public RowId OldRowId { get; set; }

		public Row NewRow { get; set; }

		string IEvent.EventMessage {
			get {
				return String.Format("Trigger '{0}' fired on '{1}' to '{2}'", TriggerName, TriggerEventType, SourceName);
			}
		}

		IDictionary<string, object> IEvent.EventData {
			get {
				return new Dictionary<string, object> {
					{"Trigger-Name", TriggerName.ToString()},
					{"Event-Type", TriggerEventType.ToString()},
					{"Old-Row-Id", OldRowId.ToString()},
					{"New-Row", NewRow}
				};
			}
		}
	}
}
