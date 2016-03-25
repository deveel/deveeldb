// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
					throw Error("Could not find the name of the trigger to delete.");

				TriggerName = idNode.Name;
			} else if (node.NodeName == "drop_callback_trigger") {
				CallbackTrigger = true;

				var tableNameNode = node.FindNode<ObjectNameNode>();
				if (tableNameNode == null)
					throw Error("Could not find the name of the table in a DROP CALLBACK TRIGGER");

				TableName = tableNameNode.Name;
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			if (CallbackTrigger) {
				var tableName = ObjectName.Parse(TableName);
				builder.AddObject(new DropCallbackTriggersStatement(tableName));
			} else {
				var triggerName = ObjectName.Parse(TriggerName);
				builder.AddObject(new DropTriggerStatement(triggerName));
			}
		}
	}
}