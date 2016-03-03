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
