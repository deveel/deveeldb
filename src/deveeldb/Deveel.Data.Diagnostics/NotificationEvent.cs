using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	[Serializable]
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
