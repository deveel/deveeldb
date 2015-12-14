using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public sealed class SessionEvent : Event {
		public SessionEvent(string userName, int commitId, SessionEventType eventType) {
			UserName = userName;
			CommitId = commitId;
			EventType = eventType;
		}

		public string UserName { get; private set; }

		public int CommitId { get; private set; }

		public SessionEventType EventType { get; private set; }

		protected override IEventSource OnSourceSet(IEventSource eventSource) {
			if (!(eventSource is IDatabase))
				throw new ArgumentException("Session event sources can be only databases");

			return base.OnSourceSet(eventSource);
		}

		protected override void GetEventData(Dictionary<string, object> data) {
			data[KnownEventMetadata.UserName] = UserName;
			data[KnownEventMetadata.CommitId] = CommitId;
			data["session.eventType"] = EventType.ToString();
		}
	}
}
