using System;

namespace Deveel.Data.Sql.Statements {
	public sealed class RollbackStatement : SqlStatement {
		protected override void ExecuteStatement(ExecutionContext context) {
			context.Request.Query.Rollback();
		}
	}
}