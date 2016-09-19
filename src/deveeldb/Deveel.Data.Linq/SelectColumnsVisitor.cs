using System;

using Deveel.Data.Sql.Expressions;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;

namespace Deveel.Data.Linq {
	class SelectColumnsVisitor : QueryModelVisitorBase {
		public SelectColumnsVisitor(ExpressionCompileContext context) {
			Context = context;
		}

		public ExpressionCompileContext Context { get; private set; }

		public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel) {
			var visitor = new SqlSelectExpressionVisitor(Context);
			visitor.Visit(selectClause.Selector);

			base.VisitSelectClause(selectClause, queryModel);
		}

		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index) {
			bool handled = true;

			if (Context.Columns.Count > 0) {
				var column = Context.Columns[index];
				var expression = column.Expression;


				if (resultOperator is CountResultOperator) {
					expression = SqlExpression.FunctionCall("COUNT", new[] {expression});
				} else if (resultOperator is FirstResultOperator) {
					// TODO: emit a LIMIT(0,1) clause
				} else {
					handled = false;
				}

				Context.Columns[index] = new SelectColumn(expression, column.Alias);
			} else {
				if (resultOperator is CountResultOperator) {
					// TODO: COUNT(*) for the proper source
				} else if (resultOperator is FirstResultOperator) {
					// TODO: emit a LIMIT(0,1) clause
				} else {
					handled = false;
				}
			}

			if (!handled)
				throw new NotSupportedException();

			base.VisitResultOperator(resultOperator, queryModel, index);
		}
	}
}
