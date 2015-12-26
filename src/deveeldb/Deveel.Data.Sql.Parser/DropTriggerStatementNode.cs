using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DropTriggerStatementNode : SqlStatementNode {
		public string TriggerName { get; private set; }

		public string TableName { get; private set; }

		public bool CallbackTrigger { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "drop_procedure_trigger") {
				var idNode = node.FindNode<ObjectNameNode>();
				if (idNode == null)
					throw new SqlParseException("Could not find the name of the trigger to delete.");

				TriggerName = idNode.Name;
			} else if (node.NodeName == "drop_callback_trigger") {
				CallbackTrigger = true;

				var tableNameNode = node.FindNode<ObjectNameNode>();
				if (tableNameNode == null)
					throw new SqlParseException("Could not find the name of the table in a DROP CALLBACK TRIGGER");

				TableName = tableNameNode.Name;
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (CallbackTrigger) {
				var tableName = ObjectName.Parse(TableName);
				builder.Objects.Add(new DropCallbackTriggersStatement(tableName));
			} else {
				var triggerName = ObjectName.Parse(TriggerName);
				builder.Objects.Add(new DropTriggerStatement(triggerName));
			}
		}
	}
}