using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Deveel.Data.Diagnostics {
	[Serializable]
	public class DebugEvent : NotificationEvent {
		public DebugEvent(StackTrace trace, int eventClass, int eventCode, string eventMessage) 
			: base(NotificationLevel.Debug, eventClass, eventCode, eventMessage) {
			Trace = trace;
		}

		public StackTrace Trace { get; private set; }

		protected override void FillEventData(IDictionary<string, object> eventData) {
			eventData["Trace"] = Trace;
		}
	}
}
