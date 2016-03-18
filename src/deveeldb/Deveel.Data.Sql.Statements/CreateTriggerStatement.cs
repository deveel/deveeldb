using System;

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateTriggerStatement : SqlStatement {
		public CreateTriggerStatement(ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body, TriggerEventType eventType) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (body == null)
				throw new ArgumentNullException("body");

			TriggerName = triggerName;
			TableName = tableName;
			Body = body;
			EventType = eventType;
		}

		public ObjectName TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public PlSqlBlockStatement Body { get; private set; }
	}
}
