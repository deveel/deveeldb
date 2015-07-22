// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Protocol {
	[Serializable]
	public sealed class TriggerEventNotification : IMessage {
		public TriggerEventNotification(string triggerName, string objectName, TriggerType triggerType, TriggerEventType eventType, int invokeCount) {
			InvokeCount = invokeCount;
			EventType = eventType;
			ObjectName = objectName;
			TriggerType = triggerType;
			TriggerName = triggerName;
		}

		public string TriggerName { get; private set; }

		public string ObjectName { get; private set; }

		public TriggerType TriggerType { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public int InvokeCount { get; private set; }

		// TODO: Provide the old state and the new state
	}
}