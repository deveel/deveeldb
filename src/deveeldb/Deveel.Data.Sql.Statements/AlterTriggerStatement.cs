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
using System.Collections.Generic;
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AlterTriggerStatement : SqlStatement {
		public AlterTriggerStatement(ObjectName triggerName, IAlterTriggerAction action) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (action == null)
				throw new ArgumentNullException("action");

			TriggerName = triggerName;
			Action = action;
		}

		private AlterTriggerStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TriggerName = (ObjectName) info.GetValue("TriggerName", typeof(ObjectName));
			Action = (IAlterTriggerAction) info.GetValue("Action", typeof(IAlterTriggerAction));
		}

		public ObjectName TriggerName { get; private set; }

		public IAlterTriggerAction Action { get; private set; }

		protected override void ConfigureSecurity(ExecutionContext context) {
			context.Assertions.AddAlter(TriggerName, DbObjectType.Trigger);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var triggerName = context.Access().ResolveObjectName(DbObjectType.Trigger, TriggerName);
			var action = Action;
			if (action is IStatementPreparable)
				action = (IAlterTriggerAction) (action as IStatementPreparable).Prepare(context);

			return new AlterTriggerStatement(triggerName, action);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.TriggerExists(TriggerName))
				throw new ObjectNotFoundException(TriggerName);
			//if (!context.User.CanAlter(DbObjectType.Trigger, TriggerName))
			//	throw new MissingPrivilegesException(context.User.Name, TriggerName, Privileges.Alter);

			var trigger = context.DirectAccess.GetObject(DbObjectType.Trigger, TriggerName) as Trigger;
			if (trigger == null)
				throw new ObjectNotFoundException(TriggerName);

			var triggerInfo = trigger.TriggerInfo;

			if (Action.ActionType == AlterTriggerActionType.Rename) {
				var action = (RenameTriggerAction) Action;
				triggerInfo = triggerInfo.Rename(action.Name);

				if (!context.DirectAccess.DropTrigger(TriggerName))
					throw new InvalidOperationException(String.Format("Could not drop the trigger '{0}' to rename", TriggerName));

				context.DirectAccess.CreateTrigger(triggerInfo);
			} else if (Action.ActionType == AlterTriggerActionType.ChangeStatus) {
				var action = (ChangeTriggerStatusAction) Action;
				triggerInfo.Status = action.Status;

				context.DirectAccess.AlterObject(triggerInfo);
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("ALTER TRIGGER ");
			TriggerName.AppendTo(builder);
			builder.Append(" ");
			Action.AppendTo(builder);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TriggerName", TriggerName);
			info.AddValue("Action", Action);
		}
	}
}
