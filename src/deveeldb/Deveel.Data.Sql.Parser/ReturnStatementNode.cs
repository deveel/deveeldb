using System;

namespace Deveel.Data.Sql.Parser {
	class ReturnStatementNode : SqlNode, IStatementNode {
		public IExpressionNode ReturnExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode)
				ReturnExpression = (IExpressionNode) node;

			return base.OnChildNode(node);
		}
	}
}
