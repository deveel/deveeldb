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
		internal TriggerException(Trigger trigger, Exception innerException)
			: this(FormMessage(trigger), innerException) {
		}

		public TriggerException(string message)
			: this(message, null) {
		}

		public TriggerException(string message, Exception innerException)
			: base(-1, message, innerException) {
		}

		private static string FormMessage(Trigger trigger) {
			return FormMessage(trigger.TriggerInfo.TableName, trigger.TriggerInfo.TriggerName, trigger.TriggerInfo.EventTime, trigger.TriggerInfo.EventType);
		}

		private static string FormMessage(ObjectName tableName, ObjectName triggerName, TriggerEventTime eventTime, TriggerEventType eventType) {
			return String.Format("An error occurred when firing trigger '{0}' {1} {2} on table '{3}'",
				triggerName, eventTime, eventType, tableName);
		}
	}
}
