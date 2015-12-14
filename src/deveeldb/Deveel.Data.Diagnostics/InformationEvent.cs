using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public class InformationEvent : Event {
		public InformationEvent(string message, InformationLevel level) {
			Message = message;
			Level = level;
		}

		public string Message { get; private set; }

		public InformationLevel Level { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["info.message"] = Message;
			data["info.level"] = Level;
		}
	}
}
