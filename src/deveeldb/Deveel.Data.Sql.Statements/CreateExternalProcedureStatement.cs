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
	public sealed class CreateExternalProcedureStatement : SqlStatement {
		public CreateExternalProcedureStatement(ObjectName procedureName, string externalReference) 
			: this(procedureName, null, externalReference) {
		}

		public CreateExternalProcedureStatement(ObjectName procedureName, IEnumerable<RoutineParameter> parameters, string externalRef) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");
			if (String.IsNullOrEmpty(externalRef))
				throw new ArgumentNullException("externalRef");

			ProcedureName = procedureName;
			Parameters = parameters;
			ExternalReference = externalRef;
		}

		public ObjectName ProcedureName { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public string ExternalReference { get; private set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			return base.PrepareExpressions(preparer);
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

			return new CreateExternalProcedureStatement(functionName, parameters, ExternalReference) {
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

			ExternalRef externRef;
			if (!ExternalRef.TryParse(ExternalReference, out externRef))
				throw new FormatException(String.Format("The external reference '{0}' is not valid.", ExternalReference));

			var functionInfo = new ExternalProcedureInfo(ProcedureName, parameters, externRef) {
				Owner = context.User.Name
			};

			context.DirectAccess.CreateRoutine(functionInfo);
			context.DirectAccess.GrantOn(DbObjectType.Routine, ProcedureName, context.User.Name, Privileges.Execute, true);
		}
	}
}
