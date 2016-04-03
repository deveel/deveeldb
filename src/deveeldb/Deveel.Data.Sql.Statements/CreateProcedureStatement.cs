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
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateProcedureStatement : SqlStatement {
		public CreateProcedureStatement(ObjectName procedureName, PlSqlBlockStatement body) 
			: this(procedureName, null, body) {
		}

		public CreateProcedureStatement(ObjectName procedureName, IEnumerable<RoutineParameter> parameters, PlSqlBlockStatement body) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");
			if (body == null)
				throw new ArgumentNullException("body");

			ProcedureName = procedureName;
			Parameters = parameters;
			Body = body;
		}

		public ObjectName ProcedureName { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public PlSqlBlockStatement Body { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var schemaName = context.Access().ResolveSchemaName(ProcedureName.ParentName);
			var functionName = new ObjectName(schemaName, ProcedureName.Name);

			var body = (PlSqlBlockStatement)Body.Prepare(context);
			return new CreateProcedureStatement(functionName, Parameters, body) {
				ReplaceIfExists = ReplaceIfExists
			};
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanCreateInSchema(ProcedureName.ParentName))
				throw new SecurityException();

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
			context.DirectAccess.GrantOn(DbObjectType.Routine, ProcedureName, context.User.Name, Privileges.Execute, true);
		}
	}
}
