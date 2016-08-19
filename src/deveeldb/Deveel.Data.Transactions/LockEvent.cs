using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class LockEvent : Event {
		internal LockEvent(LockEventType eventType, IEnumerable<ObjectName> references, LockingMode mode, AccessType accessType) {
			EventType = eventType;
			References = references;
			Mode = mode;
			AccessType = accessType;
		}

		public LockEventType EventType { get; private set; }

		public IEnumerable<ObjectName> References { get; private set; }

		public LockingMode Mode { get; private set; }

		public AccessType AccessType { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["lock.type"] = EventType.ToString().ToLowerInvariant();
			data["lock.mode"] = Mode.ToString().ToLowerInvariant();
			data["lock.access"] = AccessType.ToString().ToLowerInvariant();

			var refs = new List<ObjectName>(References);
			for (int i = 0; i < refs.Count; i++) {
				data[String.Format("lock.ref[{0}]", i)] = refs[i].ToString();
			}
		}
	}
}
