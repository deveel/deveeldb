// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateCallbackTriggerStatement : SqlStatement {
		public CreateCallbackTriggerStatement(string triggerName, ObjectName tableName, TriggerEventType eventType) {
			if (String.IsNullOrEmpty(triggerName))
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TriggerName = triggerName;
			TableName = tableName;
			EventType = eventType;
		}

		public string TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access.ResolveTableName(TableName);

			return new CreateCallbackTriggerStatement(TriggerName, tableName, EventType);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Request.Access.TableExists(TableName))
				throw new ObjectNotFoundException(TableName);

			if (context.DirectAccess.TriggerExists(new ObjectName(TriggerName)))
				throw new StatementException();

			context.Request.Access.CreateCallbackTrigger(TriggerName, TableName, EventType);
		}
	}
}
