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

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateTriggerStatement : SqlStatement {
		public CreateTriggerStatement(ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body, TriggerEventTime eventTime, TriggerEventType eventType) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (body == null)
				throw new ArgumentNullException("body");

			TriggerName = triggerName;
			TableName = tableName;
			Body = body;
			EventTime = eventTime;
			EventType = eventType;
		}

		public ObjectName TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public PlSqlBlockStatement Body { get; private set; }

		public TriggerEventTime EventTime { get; set; }

		public bool ReplaceIfExists { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var body = Body;
			if (body != null)
				body = (PlSqlBlockStatement) body.Prepare(preparer);

			return new CreateTriggerStatement(TriggerName, TableName, body, EventTime, EventType);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var schemaName = context.Access().ResolveSchemaName(TriggerName.ParentName);
			var triggerName = new ObjectName(schemaName, TriggerName.Name);

			var tableName = context.Access().ResolveTableName(TableName);

			return new CreateTriggerStatement(triggerName, tableName, Body, EventTime, EventType);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanCreateInSchema(TriggerName.ParentName))
				throw new SecurityException(String.Format("The user '{0}' cannot create in schema '{1}'.", context.User.Name, TriggerName.ParentName));

			if (!context.DirectAccess.TableExists(TableName))
				throw new ObjectNotFoundException(TableName);

			// TODO: Discover the accessed objects in the Body and verifies the user has the rights

			if (context.DirectAccess.ObjectExists(DbObjectType.Trigger, TriggerName)) {
				if (!ReplaceIfExists)
					throw new StatementException(String.Format("A trigger named '{0}' already exists.", TriggerName));

				context.DirectAccess.DropObject(DbObjectType.Trigger, TriggerName);
			}

			var triggerInfo = new PlSqlTriggerInfo(TriggerName, TableName, EventTime, EventType, Body);

			context.DirectAccess.CreateObject(triggerInfo);
			context.DirectAccess.GrantOn(DbObjectType.Trigger, TableName, context.User.Name, PrivilegeSets.SchemaAll, true);
		}
	}
}
