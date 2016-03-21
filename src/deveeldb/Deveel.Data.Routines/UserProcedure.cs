using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	public sealed class UserProcedure : Procedure {
		public UserProcedure(ProcedureInfo procedureInfo) 
			: base(procedureInfo) {
			AssertHasBody();
		}

		private void AssertHasBody() {
			if (ProcedureInfo.Body == null)
				throw new InvalidOperationException(String.Format("The user function {0} has no body.", ProcedureName));
		}

		public override InvokeResult Execute(InvokeContext context) {
			var execContext = new ExecutionContext(context.Request, ProcedureInfo.Body);
			ProcedureInfo.Body.Execute(execContext);
			return new InvokeResult(context);
		}
	}
}
