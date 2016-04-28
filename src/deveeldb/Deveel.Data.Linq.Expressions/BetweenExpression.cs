using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class BetweenExpression : QueryExpression {
		public BetweenExpression(Expression expression, Expression lower, Expression upper)
			: base(QueryExpressionType.Between, expression.Type) {
			Expression = expression;
			Lower = lower;
			Upper = upper;
		}

		public Expression Expression { get; private set; }

		public Expression Lower { get; private set; }

		public Expression Upper { get; private set; }
	}
}
