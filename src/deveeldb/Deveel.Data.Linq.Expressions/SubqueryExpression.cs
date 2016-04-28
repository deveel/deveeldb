using System;

namespace Deveel.Data.Linq.Expressions {
	public abstract class SubqueryExpression : QueryExpression {
		protected SubqueryExpression(QueryExpressionType nodeType, Type type, SelectExpression query)
			: base(nodeType, type) {
			if (nodeType != QueryExpressionType.In &&
				nodeType != QueryExpressionType.Exists &&
				nodeType != QueryExpressionType.Scalar)
				throw new ArgumentException(String.Format("The node type '{0}' is invalid as sub-query.", nodeType));

			Query = query;
		}

		public SelectExpression Query { get; private set; }
	}
}
