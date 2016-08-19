using System;

namespace Deveel.Data.Sql.Expressions {
	public sealed class ExpressionParseResult {
		private ExpressionParseResult(SqlExpression expression, string[] errors, bool valid) {
			Expression = expression;
			Errors = errors;
			IsValid = valid;
		}

		public ExpressionParseResult(SqlExpression expression)
			: this(expression, expression == null ? new[] {"An unknown error occurred while parsing the exception"} : new string[0],
				expression != null) {
		}

		public ExpressionParseResult(string[] errors)
			: this(null, errors, false) {
		}

		public SqlExpression Expression { get; private set; }

		public string[] Errors { get; private set; }

		public bool IsValid { get; private set; }
	}
}
