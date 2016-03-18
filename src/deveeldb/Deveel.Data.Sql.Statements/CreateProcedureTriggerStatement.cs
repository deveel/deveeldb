using System;

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateProcedureTriggerStatement : SqlStatement {
		public CreateProcedureTriggerStatement(ObjectName triggerName, ObjectName tableName, string procedureName, TriggerEventType eventType) 
			: this(triggerName, tableName, procedureName, false, eventType) {
		}

		public CreateProcedureTriggerStatement(ObjectName triggerName, ObjectName tableName, string procedureName, bool external, TriggerEventType eventType) {
			TriggerName = triggerName;
			TableName = tableName;
			ProcedureName = procedureName;
			IsExternal = external;
			EventType = eventType;
		}

		public ObjectName TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public string ProcedureName { get; private set; }

		public bool IsExternal { get; private set; }
	}
}
