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
				throw new FormatException(String.Format("The external reference '{0}' is not valid."));

			var functionInfo = new FunctionInfo(FunctionName, parameters, ReturnType, FunctionType.UserDefined) {
				ExternalRef = externRef,
				Owner = context.User.Name
			};

			context.DirectAccess.CreateRoutine(functionInfo);
			context.DirectAccess.GrantOn(DbObjectType.Routine, FunctionName, context.User.Name, Privileges.Execute, true);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var functionName = context.Access.ResolveSchemaName(FunctionName.FullName);

			return new CreateExternalFunctionStatement(functionName, ReturnType, Parameters, ExternalReference);
		}
	}
}
