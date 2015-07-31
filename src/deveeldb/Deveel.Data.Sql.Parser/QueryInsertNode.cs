using System;

namespace Deveel.Data.Sql.Parser {
	class QueryInsertNode : SqlNode {
		public SqlQueryExpressionNode QueryExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlQueryExpressionNode)
				QueryExpression = (SqlQueryExpressionNode) node;

			return base.OnChildNode(node);
		}
	}
}
