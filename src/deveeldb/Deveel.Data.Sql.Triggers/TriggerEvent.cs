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
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerEvent : Event {
		internal TriggerEvent(TriggerType triggerType, ObjectName triggerName, ObjectName sourceName, TriggerEventTime eventTime, TriggerEventType eventType, RowId oldRowId, Row newRow) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (sourceName == null)
				throw new ArgumentNullException("sourceName");

			TriggerType = triggerType;
			TriggerName = triggerName;
			SourceName = sourceName;
			EventTime = eventTime;
			EventType = eventType;
			OldRowId = oldRowId;
			NewRow = newRow;
		}

		public TriggerType TriggerType { get; private set; }

		public ObjectName TriggerName { get; private set; }

		public ObjectName SourceName { get; private set; }

		public TriggerEventTime EventTime { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public RowId OldRowId { get; set; }

		public Row NewRow { get; set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["trigger.name"] = TriggerName.FullName;
			data["trigger.eventTime"] = EventTime.ToString();
			data["trigger.eventType"] = EventType.ToString();
			data["trigger.source"] = SourceName.FullName;
			data["trigger.old.tableId"] = OldRowId.TableId;
			data["trigger.old.rowNumber"] = OldRowId.RowNumber;
			data["trigger.new"] = NewRow;
		}
	}
}
