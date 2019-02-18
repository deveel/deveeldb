using System;

namespace Deveel.Data.Sql.Statements {
	public sealed class StatementErrorResult : IStatementResult {
		public StatementErrorResult(SqlStatementException error) {
			Error = error ?? throw new ArgumentNullException(nameof(error));
		}

		public SqlStatementException Error { get; }
	}
}