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
using System.Runtime.Serialization;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateProcedureTriggerStatement : SqlStatement {
		public CreateProcedureTriggerStatement(ObjectName triggerName, ObjectName tableName, ObjectName procedureName, TriggerEventTime eventTime, TriggerEventType eventType) 
			: this(triggerName, tableName, procedureName, new InvokeArgument[0], eventTime, eventType) {
		}

		public CreateProcedureTriggerStatement(ObjectName triggerName, ObjectName tableName, ObjectName procedureName, InvokeArgument[] args, TriggerEventTime eventTime, TriggerEventType eventType) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");

			TriggerName = triggerName;
			TableName = tableName;
			ProcedureName = procedureName;
			ProcedureArguments = args;
			EventTime = eventTime;
			EventType = eventType;
		}

		private CreateProcedureTriggerStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TriggerName = (ObjectName) info.GetValue("TriggerName", typeof(ObjectName));
			TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
			ProcedureName = (ObjectName) info.GetValue("ProcedureName", typeof(ObjectName));
			ProcedureArguments = (InvokeArgument[]) info.GetValue("ProcedureArguments", typeof(InvokeArgument[]));
			EventTime = (TriggerEventTime) info.GetInt32("EventTime");
			EventType = (TriggerEventType) info.GetInt32("EventType");
			ReplaceIfExists = info.GetBoolean("ReplaceIfExists");
			Status = (TriggerStatus) info.GetInt32("Status");
		}

		public ObjectName TriggerName { get; private set; }

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		public ObjectName ProcedureName { get; private set; }

		public InvokeArgument[] ProcedureArguments { get; private set; }

		public TriggerEventTime EventTime { get; private set; }

		public bool ReplaceIfExists { get; set; }

		public TriggerStatus Status { get; set; }

		protected override void OnBeforeExecute(ExecutionContext context) {
			RequestCreate(TriggerName, DbObjectType.Trigger);
			RequestReference(TableName, DbObjectType.Table);
			RequestExecute(ProcedureName);

			GrantAccess(ProcedureName, DbObjectType.Routine, PrivilegeSets.RoutineAll);

			base.OnBeforeExecute(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("CREATE ");

			if (ReplaceIfExists)
				builder.Append("OR REPLACE ");

			builder.Append("TRIGGER ");
			TriggerName.AppendTo(builder);

			builder.AppendFormat(" {0} {1} ", EventTime.ToString().ToUpperInvariant(), EventType.AsDebugString());

			builder.Append("ON ");
			TableName.AppendTo(builder);
			builder.Append(" ");

			builder.Append("FOR EACH ROW ");

			if (Status != TriggerStatus.Unknown) {
				if (Status == TriggerStatus.Disabled) {
					builder.Append("DISABLE ");
				} else if (Status == TriggerStatus.Enabled) {
					builder.Append("ENABLE ");
				}
			}

			builder.Append("CALL ");

			ProcedureName.AppendTo(builder);
			builder.Append("(");

			if (ProcedureArguments != null &&
			    ProcedureArguments.Length > 0) {
				for (int i = 0; i < ProcedureArguments.Length; i++) {
					ProcedureArguments[i].AppendTo(builder);

					if (i < ProcedureArguments.Length - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var args = new InvokeArgument[ProcedureArguments == null ? 0 : ProcedureArguments.Length];
			if (ProcedureArguments != null) {
				for (int i = 0; i < args.Length; i++) {
					args[i] = (InvokeArgument) (ProcedureArguments[i] as IPreparable).Prepare(preparer);
				}
			}

			return new CreateProcedureTriggerStatement(TriggerName, TableName, ProcedureName, args, EventTime, EventType) {
				Status = Status,
				ReplaceIfExists = ReplaceIfExists
			};
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var triggerSchemaName = context.Access().ResolveSchemaName(TriggerName.ParentName);
			var triggerName = new ObjectName(triggerSchemaName, TriggerName.Name);

			var tableName = context.Access().ResolveTableName(TableName);
			var procedureName = context.Access().ResolveObjectName(DbObjectType.Routine, ProcedureName);

			return new CreateProcedureTriggerStatement(triggerName, tableName, procedureName, ProcedureArguments, EventTime,
				EventType) {
					ReplaceIfExists = ReplaceIfExists,
					Status = Status
				};
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			//if (!context.User.CanCreateInSchema(TriggerName.ParentName))
			//	throw new SecurityException(String.Format("The user '{0}' cannot create in schema '{1}'.", context.User.Name, TriggerName.ParentName));

			//if (!context.User.CanExecuteProcedure(new Invoke(ProcedureName, ProcedureArguments), context.Request))
			//	throw new MissingPrivilegesException(context.User.Name, ProcedureName, Privileges.Execute);

			if (!context.DirectAccess.TableExists(TableName))
				throw new ObjectNotFoundException(TableName);

			if (context.DirectAccess.ObjectExists(DbObjectType.Trigger, TriggerName)) {
				if (!ReplaceIfExists)
					throw new StatementException(String.Format("A trigger named '{0}' already exists.", TriggerName));

				context.DirectAccess.DropObject(DbObjectType.Trigger, TriggerName);
			}

			var triggerInfo = new ProcedureTriggerInfo(TriggerName, TableName, EventTime, EventType, ProcedureName, ProcedureArguments);

			if (Status != TriggerStatus.Unknown)
				triggerInfo.Status = Status;

			context.DirectAccess.CreateObject(triggerInfo);
			//context.DirectAccess.GrantOn(DbObjectType.Trigger, TableName, context.User.Name, PrivilegeSets.SchemaAll, true);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TriggerName", TriggerName);
			info.AddValue("TableName", TableName);
			info.AddValue("ProcedureName", ProcedureName);
			info.AddValue("ProcedureArguments", ProcedureArguments);
			info.AddValue("EventTime", (int)EventTime);
			info.AddValue("EventType", (int)EventType);
			info.AddValue("ReplaceIfExists", ReplaceIfExists);
			info.AddValue("Status", (int)Status);
		}
	}
}
