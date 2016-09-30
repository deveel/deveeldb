// 
//  Copyright 2010-2016 Deveel
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
