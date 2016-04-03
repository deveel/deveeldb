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
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateExternalFunctionStatement : SqlStatement {
		public CreateExternalFunctionStatement(ObjectName functionName, SqlType returnType, string externalRef) 
			: this(functionName, returnType, null, externalRef) {
		}

		public CreateExternalFunctionStatement(ObjectName functionName, SqlType returnType,  IEnumerable<RoutineParameter> parameters, string externalRef) {
			if (functionName == null)
				throw new ArgumentNullException("functionName");
			if (returnType == null)
				throw new ArgumentNullException("returnType");
			if (String.IsNullOrEmpty(externalRef))
				throw new ArgumentNullException("externalRef");

			FunctionName = functionName;
			ReturnType = returnType;
			Parameters = parameters;
			ExternalReference = externalRef;
		}

		public ObjectName FunctionName { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public SqlType ReturnType { get; private set; }

		public bool ReplaceIfExists { get; set; }

		public string ExternalReference { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanCreateInSchema(FunctionName.ParentName))
				throw new SecurityException();

			if (context.DirectAccess.RoutineExists(FunctionName)) {
				if (!ReplaceIfExists)
					throw new StatementException(String.Format("A routine named '{0}' already exists in the database.", FunctionName));

				context.DirectAccess.DeleteRoutine(FunctionName);
			}

			var parameters = new RoutineParameter[0];
			if (Parameters != null)
				parameters = Parameters.ToArray();

			ExternalRef externRef;
			if (!ExternalRef.TryParse(ExternalReference, out externRef))
				throw new FormatException(String.Format("The external reference '{0}' is not valid.", ExternalReference));

			var functionInfo = new ExternalFunctionInfo(FunctionName, parameters, ReturnType, externRef) {
				Owner = context.User.Name
			};

			context.DirectAccess.CreateRoutine(functionInfo);
			context.DirectAccess.GrantOn(DbObjectType.Routine, FunctionName, context.User.Name, Privileges.Execute, true);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var schemaName = context.Access().ResolveSchemaName(FunctionName.ParentName);
			var functionName = new ObjectName(schemaName, FunctionName.Name);

			return new CreateExternalFunctionStatement(functionName, ReturnType, Parameters, ExternalReference);
		}
	}
}
