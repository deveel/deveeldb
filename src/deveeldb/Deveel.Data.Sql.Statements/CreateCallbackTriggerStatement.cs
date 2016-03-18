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

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access.ResolveTableName(TableName);

			return new CreateCallbackTriggerStatement(tableName, EventType);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Request.Access.TableExists(TableName))
				throw new ObjectNotFoundException(TableName);

			context.Request.Access.CreateCallbackTrigger(TableName, EventType);
		}
	}
}
