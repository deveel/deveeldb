using System;

namespace Deveel.Data.Sql.Statements {
	public abstract class SqlPreparedStatement : SqlStatement, IPreparedStatement {
		void IExecutable.Execute(ExecutionContext context) {
			ExecuteStatement(context);
		}

		protected abstract void ExecuteStatement(ExecutionContext context);
	}
}
