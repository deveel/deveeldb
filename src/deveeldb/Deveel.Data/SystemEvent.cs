using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	public sealed class SystemEvent : Event {
		public SystemEvent(SystemEventType eventType, IQuery systemQuery) {
			EventType = eventType;
			SystemQuery = systemQuery;
		}

		public IQuery SystemQuery { get; private set; }

		public SystemEventType EventType { get; private set; }
	}
}
