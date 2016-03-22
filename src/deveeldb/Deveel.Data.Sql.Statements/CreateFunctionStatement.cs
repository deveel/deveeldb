using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateFunctionStatement : SqlStatement {
		public CreateFunctionStatement(ObjectName functionName, SqlType returnType, PlSqlBlockStatement body) 
			: this(functionName, returnType, null, body) {
		}

		public CreateFunctionStatement(ObjectName functionName, SqlType returnType, IEnumerable<RoutineParameter> parameters, PlSqlBlockStatement body) {
			if (functionName == null)
				throw new ArgumentNullException("functionName");
			if (returnType == null)
				throw new ArgumentNullException("returnType");
			if (body == null)
				throw new ArgumentNullException("body");

			FunctionName = functionName;
			ReturnType = returnType;
			Parameters = parameters;
			Body = body;
		}

		public ObjectName FunctionName { get; private set; }

		public SqlType ReturnType { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public PlSqlBlockStatement Body { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var body = (PlSqlBlockStatement) Body.Prepare(preparer);
			return new CreateFunctionStatement(FunctionName, ReturnType, Parameters, body) {
				ReplaceIfExists = ReplaceIfExists
			};
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var schemaName = context.Access.ResolveSchemaName(FunctionName.ParentName);
			var functionName = new ObjectName(schemaName, FunctionName.Name);

			var body = (PlSqlBlockStatement) Body.Prepare(context);
			return new CreateFunctionStatement(functionName, ReturnType, Parameters, body) {
				ReplaceIfExists = ReplaceIfExists
			};
		}

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

			var functionInfo = new FunctionInfo(FunctionName, parameters, ReturnType, FunctionType.UserDefined) {
				Body = Body,
				Owner = context.User.Name
			};

			context.DirectAccess.CreateRoutine(functionInfo);
			context.DirectAccess.GrantOn(DbObjectType.Routine, FunctionName, context.User.Name, Privileges.Execute, true);
		}
	}
}
