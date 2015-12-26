using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql.Statements {
	public sealed class CommitStatement : SqlStatement {
		protected override void ExecuteStatement(ExecutionContext context) {
			context.Request.Query.Commit();
		}
	}
}