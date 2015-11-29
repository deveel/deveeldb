using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public class PerformanceEvent : Event {
		public PerformanceEvent(string counterKey, object value) {
			if (String.IsNullOrEmpty(counterKey))
				throw new ArgumentNullException("counterKey");

			CounterKey = counterKey;
			Value = value;
		}

		public string CounterKey { get; private set; }

		public object Value { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data[CounterKey] = Value;
		}
	}
}
