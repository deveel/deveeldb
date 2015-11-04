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

namespace Deveel.Data.Sql.Triggers {
	public class TableEventException : SqlErrorException {
		public TableEventException(TableEventContext tableEvent)
			: this(tableEvent, FormMessage(tableEvent)) {
		}

		public TableEventException(TableEventContext tableEvent, Exception innerException)
			: this(tableEvent, FormMessage(tableEvent), innerException) {
		}

		public TableEventException(TableEventContext tableEvent, string message)
			: this(tableEvent, message, null) {
		}

		public TableEventException(TableEventContext tableEvent, string message, Exception innerException)
			: base(-1, message, innerException) {
			TableName = tableEvent.Table.FullName;
			EventType = tableEvent.EventType;
		}

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		private static string FormMessage(TableEventContext tableEvent) {
			return String.Format("An error occurred when firing triggers '{0}' on '{1}'.", tableEvent.EventType.AsString(),
				tableEvent.Table.FullName);
		}
	}
}
