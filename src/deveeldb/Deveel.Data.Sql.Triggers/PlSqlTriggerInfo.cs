using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Triggers {
	public sealed class PlSqlTriggerInfo : TriggerInfo {
		public PlSqlTriggerInfo(ObjectName triggerName, ObjectName tabbleName, TriggerEventType eventTypes, PlSqlBlockStatement body) 
			: base(triggerName, tabbleName, eventTypes) {
			if (body == null)
				throw new ArgumentNullException("body");

			Body = body;
		}

		public PlSqlBlockStatement Body { get; private set; }
	}
}
