using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ReturnStatement : SqlStatement, IPlSqlStatement {
		public ReturnStatement() 
			: this(null) {
		}

		public ReturnStatement(SqlExpression returnExpression) {
			ReturnExpression = returnExpression;
		}

		public SqlExpression ReturnExpression { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var expression = ReturnExpression;
			if (expression != null)
				expression = expression.Prepare(preparer);

			return new ReturnStatement(expression);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			context.Return(ReturnExpression);
		}
	}
}
