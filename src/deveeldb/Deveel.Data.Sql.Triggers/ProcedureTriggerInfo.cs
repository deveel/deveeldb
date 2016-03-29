using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Triggers {
	public sealed class ProcedureTriggerInfo : TriggerInfo {
		public ProcedureTriggerInfo(ObjectName triggerName, ObjectName tabbleName, TriggerEventType eventTypes, ObjectName procedureName) 
			: this(triggerName, tabbleName, eventTypes, procedureName, new SqlExpression[0]) {
		}

		public ProcedureTriggerInfo(ObjectName triggerName, ObjectName tabbleName, TriggerEventType eventTypes, ObjectName procedureName, SqlExpression[] args) 
			: base(triggerName, tabbleName, eventTypes) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");

			ProcedureName = procedureName;
			Arguments = args;
		}

		public ObjectName ProcedureName { get; private set; }

		public SqlExpression[] Arguments { get; set; }
	}
}
