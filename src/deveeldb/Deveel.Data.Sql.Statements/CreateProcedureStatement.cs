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
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateProcedureStatement : SqlStatement {
		public CreateProcedureStatement(ObjectName procedureName, SqlStatement body) 
			: this(procedureName, null, body) {
		}

		public CreateProcedureStatement(ObjectName procedureName, RoutineParameter[] parameters, SqlStatement body) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");
			if (body == null)
				throw new ArgumentNullException("body");

			ProcedureName = procedureName;
			Parameters = parameters;
			Body = body;
		}

		public ObjectName ProcedureName { get; private set; }

		public RoutineParameter[] Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public SqlStatement Body { get; private set; }

		protected override void OnBeforeExecute(ExecutionContext context) {
			RequestCreate(ProcedureName, DbObjectType.Routine);
			GrantAccess(ProcedureName, DbObjectType.Routine, PrivilegeSets.RoutineAll);

			base.OnBeforeExecute(context);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var schemaName = context.Access().ResolveSchemaName(ProcedureName.ParentName);
			var functionName = new ObjectName(schemaName, ProcedureName.Name);

			var parameters = new List<RoutineParameter>();
			if (Parameters != null) {
				foreach (var parameter in Parameters) {
					parameters.Add((RoutineParameter)((IStatementPreparable)parameter).Prepare(context));
				}
			}

			var body = (PlSqlBlockStatement)Body.Prepare(context);
			return new CreateProcedureStatement(functionName, parameters.ToArray(), body) {
				ReplaceIfExists = ReplaceIfExists
			};
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			//if (!context.User.CanCreateInSchema(ProcedureName.ParentName))
			//	throw new SecurityException();

			if (context.DirectAccess.RoutineExists(ProcedureName)) {
				if (!ReplaceIfExists)
					throw new StatementException(String.Format("A routine named '{0}' already exists in the database.", ProcedureName));

				context.DirectAccess.DeleteRoutine(ProcedureName);
			}

			var parameters = new RoutineParameter[0];
			if (Parameters != null)
				parameters = Parameters.ToArray();

			var functionInfo = new PlSqlProcedureInfo(ProcedureName, parameters, Body) {
				Owner = context.User.Name
			};

			context.DirectAccess.CreateRoutine(functionInfo);
			//context.DirectAccess.GrantOn(DbObjectType.Routine, ProcedureName, context.User.Name, Privileges.Execute, true);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			var orReplace = ReplaceIfExists ? "OR REPLACE" : "";
			builder.AppendFormat("CREATE {0}PROCEDURE ", orReplace);
			ProcedureName.AppendTo(builder);

			builder.Append("(");
			if (Parameters != null && Parameters.Length > 0) {
				for (int i = 0; i < Parameters.Length; i++) {
					Parameters[i].AppendTo(builder);

					if (i < Parameters.Length - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");

			builder.AppendLine(" IS");

			builder.Indent();

			Body.AppendTo(builder);

			builder.DeIndent();
		}
	}
}
