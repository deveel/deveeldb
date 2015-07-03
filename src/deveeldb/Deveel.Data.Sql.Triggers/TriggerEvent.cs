using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerEvent : IEvent {
		private readonly Dictionary<string, object> data;

		public TriggerEvent(ObjectName triggerName, TriggerEventType eventType, int fireCount) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");

			data = new Dictionary<string, object>();

			this.SetData("Trigger-Name", triggerName.FullName);
			this.SetData("Event-Type", eventType);
			this.SetData("Fire-Count", fireCount);
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

		public ObjectName TriggerName {
			get {
				var triggerName = this.GetData<string>("Trigger-Name");
				return ObjectName.Parse(triggerName);
			}
		}

		public TriggerEventType TriggerEventType {
			get { return this.GetData<TriggerEventType>("Event-Type"); }
		}

		public int FireCount {
			get { return this.GetData<int>("Fire-Count"); }
		}

		string IEvent.EventMessage {
			get {
				//TODO:
				return null;
			}
		}

		IDictionary<string, object> IEvent.EventData {
			get { return data; }
		}
	}
}
