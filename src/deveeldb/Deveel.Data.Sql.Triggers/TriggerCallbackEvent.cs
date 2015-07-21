using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerCallbackEvent : IEvent {

		byte IEvent.EventType {
			get { return (byte) EventType.Trigger; }
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

		string IEvent.EventMessage {
			get { throw new NotImplementedException(); }
		}

		IDictionary<string, object> IEvent.EventData {
			get { throw new NotImplementedException(); }
		}
	}
}
