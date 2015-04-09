using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	class QueryBuilder : ExpressionVisitor {
		private TableQuery resultQuery;

		public TableQuery Build(Expression expression) {
			Visit(expression);
			return resultQuery;
		}
	}
}
