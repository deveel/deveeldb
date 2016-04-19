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
	public abstract class TriggerInfo : IObjectInfo {
		protected TriggerInfo(ObjectName triggerName, ObjectName tableName, TriggerEventType eventTypes) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TriggerName = triggerName;
			TableName = tableName;
			EventTypes = eventTypes;
		}

		public ObjectName TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventTypes { get; private set; }

		ObjectName IObjectInfo.FullName {
			get { return TriggerName; }
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Trigger; }
		}

		public string Owner { get; set; }

		public bool CanFire(TableEvent tableEvent) {
			if (!TableName.Equals(tableEvent.Table.TableInfo.TableName))
				return false;

			return MatchesEvent(tableEvent.EventType);
		}

		private bool MatchesEvent(TriggerEventType eventType) {
			if ((eventType & TriggerEventType.Before) != 0 &&
			    (EventTypes & TriggerEventType.Before) == 0)
				return false;
			if ((eventType & TriggerEventType.After) != 0 &&
			    (EventTypes & TriggerEventType.After) == 0)
				return false;

			bool matches = false;

			if ((EventTypes & TriggerEventType.AfterInsert) != 0)
				matches = (eventType & TriggerEventType.Insert) != 0;
			if ((EventTypes & TriggerEventType.Update) != 0)
				matches = (eventType & TriggerEventType.Update) != 0;
			if ((EventTypes & TriggerEventType.Delete) != 0)
				matches = (eventType & TriggerEventType.Delete) != 0;

			return matches;
		}
	}
}
