using System;

namespace Deveel.Data.Linq.Expressions {
	public sealed class ScalarExpression : SubqueryExpression {
		public ScalarExpression(Type type, SelectExpression query)
			: base(QueryExpressionType.Scalar, type, query) {
		}
	}
}
