using System;

namespace Deveel.Data.Sql.Triggers {
	public sealed class CallbackTriggerInfo : TriggerInfo {
		public CallbackTriggerInfo(string triggerName, ObjectName tableName, TriggerEventType eventType)
			: base(new ObjectName(triggerName), tableName, eventType) {
		}
	}
}
