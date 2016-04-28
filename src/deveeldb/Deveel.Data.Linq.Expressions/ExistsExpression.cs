using System;

namespace Deveel.Data.Linq.Expressions {
	public sealed class ExistsExpression : SubqueryExpression {
		public ExistsExpression(SelectExpression query)
			: base(QueryExpressionType.Exists, typeof(bool), query) {
		}
	}
}
