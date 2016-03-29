using System;

namespace Deveel.Data.Sql.Triggers {
	public interface ITriggerInfo : IObjectInfo {
		ObjectName TableName { get; }

		TriggerEventType EventType { get; }
	}
}
