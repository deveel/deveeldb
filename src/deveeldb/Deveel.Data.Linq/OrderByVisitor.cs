using System;

using Remotion.Linq;
using Remotion.Linq.Clauses;

namespace Deveel.Data.Linq {
	class OrderByVisitor : QueryModelVisitorBase {
		public OrderByVisitor(ExpressionCompileContext context) {
			Context = context;
		}

		public ExpressionCompileContext Context { get; private set; }

		public override void VisitOrdering(Ordering ordering, QueryModel queryModel, OrderByClause orderByClause, int index) {
			var sqlExpression = ExpressionToSqlExpressionVisitor.GetSqlExpression(Context, ordering.Expression);
			var ascending = ordering.OrderingDirection == OrderingDirection.Asc;

			Context.OrderBy(sqlExpression, ascending);

			base.VisitOrdering(ordering, queryModel, orderByClause, index);
		}
	}
}
