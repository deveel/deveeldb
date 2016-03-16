using System;

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateCallbackTriggerStatement : SqlStatement {
		public CreateCallbackTriggerStatement(ObjectName tableName, TriggerEventType eventType) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			EventType = eventType;
		}

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }
	}
}
