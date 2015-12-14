using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Transactions {
	public sealed class TransactionEvent : Event {
		public TransactionEvent(int commitId, TransactionEventType eventType) {
			CommitId = commitId;
			EventType = eventType;
		}

		public int CommitId { get; private set; }

		public TransactionEventType EventType { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data[KnownEventMetadata.CommitId] = CommitId;
			data["transaction.eventType"] = EventType.ToString();
		}
	}
}
