using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class OuterJoinedExpression : QueryExpression {
		public OuterJoinedExpression(Expression test, Expression expression)
			: base(QueryExpressionType.OuterJoined, expression.Type) {
			Test = test;
			Expression = expression;
		}

		public Expression Test { get; private set; }

		public Expression Expression { get; private set; }
	}
}
