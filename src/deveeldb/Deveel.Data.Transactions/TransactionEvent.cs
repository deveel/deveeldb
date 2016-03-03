// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
