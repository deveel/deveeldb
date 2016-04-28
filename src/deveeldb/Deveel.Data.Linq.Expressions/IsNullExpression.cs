using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class IsNullExpression : QueryExpression {
		public IsNullExpression(Expression expression)
			: base(QueryExpressionType.IsNull, typeof(bool)) {
			Expression = expression;
		}

		public Expression Expression { get; private set; }
	}
}
