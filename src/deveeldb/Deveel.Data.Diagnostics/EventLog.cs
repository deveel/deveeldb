using System;

namespace Deveel.Data.Diagnostics {
	[Serializable]
	public sealed class EventLog {
		public EventLog(IEvent @event, LogLevel level) 
			: this(@event, level, DateTime.UtcNow) {
		}

		public EventLog(IEvent @event, LogLevel level, DateTime date) {
			if (@event == null)
				throw new ArgumentNullException("event");

			Event = @event;
			Level = level;
			Date = date;
		}

		public LogLevel Level { get; private set; }

		public IEvent Event { get; private set; }

		public DateTime Date { get; private set; }
	}
}
