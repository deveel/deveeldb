using System;

namespace Deveel.Data.Sql.Triggers {
	public sealed class CallbackTriggerInfo : ITriggerInfo {
		public CallbackTriggerInfo(string triggerName, ObjectName tableName, TriggerEventType eventType) {
			if (String.IsNullOrEmpty(triggerName))
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TriggerName = triggerName;
			TableName = tableName;
			EventType = eventType;
		}

		public string TriggerName { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Trigger; }
		}

		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(TriggerName);}
		}

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		internal bool CanFire(TableEvent tableEvent) {
			return TableName.Equals(tableEvent.Table.FullName) &&
			       (EventType & tableEvent.EventType) != 0;
		}
	}
}
