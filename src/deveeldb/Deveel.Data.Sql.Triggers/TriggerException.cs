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

namespace Deveel.Data.Sql.Triggers {
	public class TriggerException : SqlErrorException {
		public TriggerException(Trigger trigger, Exception innerException)
			: this(trigger, FormMessage(trigger), innerException) {
		}

		public TriggerException(Trigger trigger, string message, Exception innerException)
			: base(-1, message, innerException) {
			TableName = trigger.TriggerInfo.TableName;
			TriggerName = trigger.TriggerName;
			EventType = trigger.TriggerInfo.EventType;
		}

		public TriggerException(Trigger trigger) 
			: this(trigger, FormMessage(trigger)) {
		}

		public TriggerException(Trigger trigger, string message) 
			: this(trigger, message, null) {
		}

		public ObjectName TableName { get; private set; }

		public ObjectName TriggerName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		private static string FormMessage(Trigger trigger) {
			return FormMessage(trigger.TriggerInfo.TableName, trigger.TriggerName, trigger.TriggerInfo.EventType);
		}

		private static string FormMessage(ObjectName tableName, ObjectName triggerName, TriggerEventType eventType) {
			return String.Format("An error occurred when firing trigger '{0}' on table '{1}' on {2}",
				triggerName, tableName, eventType.AsString());
		}
	}
}
