using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public abstract class Event : IEvent {
		private readonly IDictionary<string, object> metadata;
		 
		protected Event() {
			metadata = GenerateEventData();
		}

		private IDictionary<string, object> GenerateEventData() {
			var dictionary = new Dictionary<string, object>();
			GetEventData(dictionary);
			return dictionary;
		}

		protected virtual void GetEventData(Dictionary<string, object> data) {
		}

		public IEventSource EventSource { get; set; }

		IDictionary<string, object> IEvent.EventData {
			get { return metadata; }
		}
	}
}
