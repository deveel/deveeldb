using System;

namespace Deveel.Data.Sql.Statements {
	public sealed class StatementErrorResult : IStatementResult {
		public StatementErrorResult(SqlStatementException error) {
			Error = error ?? throw new ArgumentNullException(nameof(error));
		}

		public StatementErrorResult(Exception error)
			: this(new SqlStatementException("An error occurred while executing the statement", error)) {
		}

		public SqlStatementException Error { get; }
	}
}