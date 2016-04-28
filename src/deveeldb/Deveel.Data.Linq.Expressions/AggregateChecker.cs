using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	class AggregateChecker : QueryExpressionVisitor {
		private bool aggregateFound;

		protected override Expression VisitAggregate(AggregateExpression aggregate) {
			aggregateFound = true;
			return base.VisitAggregate(aggregate);
		}

		protected override Expression VisitSubquery(SubqueryExpression subquery) {
			return subquery;
		}

		protected override Expression VisitSelect(SelectExpression @select) {
			VisitQueryColumns(select.Columns);
			Visit(select.Where);
			VisitOrderBy(select.OrderBy);
			return select;
		}

		public static bool HasAggregate(Expression expression) {
			var checker = new AggregateChecker();
			checker.Visit(expression);
			return checker.aggregateFound;
		}
	}
}
