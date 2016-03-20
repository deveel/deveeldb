using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CallStatement  : SqlStatement {
		public CallStatement(ObjectName procedureName) 
			: this(procedureName, null) {
		}

		public CallStatement(ObjectName procedureName, IEnumerable<SqlExpression> arguments) {
			ProcedureName = procedureName;
			Arguments = arguments;
		}

		public ObjectName ProcedureName { get; private set; }

		public IEnumerable<SqlExpression> Arguments { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var args = Arguments;
			if (args != null) {
				var newArgs = new List<SqlExpression>();
				foreach (var arg in args) {
					newArgs.Add(arg.Prepare(preparer));
				}

				args = newArgs.ToArray();
			}

			return new CallStatement(ProcedureName, args);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var procedureName = context.Access.ResolveObjectName(DbObjectType.Routine, ProcedureName);

			return new CallStatement(procedureName, Arguments);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var args = Arguments != null ? Arguments.ToArray() : new SqlExpression[0];
			var invoke = new Invoke(ProcedureName, args);

			var procedure = context.DirectAccess.ResolveProcedure(invoke);
			if (procedure == null)
				throw new StatementException(String.Format("Could not retrieve the procedure '{0}': maybe not a procedure.", ProcedureName));

			if (!context.User.CanExecute(RoutineType.Procedure, invoke,context.Request))
				throw new MissingPrivilegesException(context.User.Name, ProcedureName, Privileges.Execute);

			procedure.Execute(context.Request);
		}
	}
}
