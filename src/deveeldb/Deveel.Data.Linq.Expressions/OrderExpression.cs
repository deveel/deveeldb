using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class OrderExpression {
		public OrderExpression(Expression expression, OrderType orderType) {
			Expression = expression;
			OrderType = orderType;
		}

		public Expression Expression { get; private set; }

		public OrderType OrderType { get; private set; }
	}
}
