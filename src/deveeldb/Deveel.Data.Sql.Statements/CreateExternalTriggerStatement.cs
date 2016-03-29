using System;

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateExternalTriggerStatement : SqlStatement {
		public CreateExternalTriggerStatement(ObjectName triggerName, ObjectName tableName, string externalReference, TriggerEventType eventType) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(externalReference))
				throw new ArgumentNullException("externalReference");

			TriggerName = triggerName;
			TableName = tableName;
			ExternalReference = externalReference;
			EventType = eventType;
		}

		public ObjectName TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public string ExternalReference { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			return base.PrepareStatement(context);
		}
	}
}
