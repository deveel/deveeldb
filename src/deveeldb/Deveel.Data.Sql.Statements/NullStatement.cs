using System;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class NullStatement : SqlStatement, IPlSqlStatement {
		protected override void ExecuteStatement(ExecutionContext context) {
		}
	}
}
