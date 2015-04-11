using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	internal class QueryBuilder : ExpressionVisitor {
		private readonly TableQuery resultQuery;

		public QueryBuilder() {
			resultQuery = new TableQuery();
		}

		public TableQuery Build(Expression expression) {
			Visit(expression);
			return resultQuery;
		}

		public TableQuery Build() {
			return Build(null);
		}

		protected override Expression VisitBinary(BinaryExpression b) {
			var expressionType = b.NodeType;
			return base.VisitBinary(b);
		}

		protected override MemberAssignment VisitMemberAssignment(MemberAssignment assignment) {
			return base.VisitMemberAssignment(assignment);
		}
	}
}
