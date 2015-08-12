using System;

namespace Deveel.Data.Sql.Parser {
	class ContinueStatementNode : SqlNode, IStatementNode {
		public string Label { get; private set; }

		public IExpressionNode WhenExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("label_opt")) {
				Label = node.FindNode<StringLiteralNode>().Value;
			} else if (node.NodeName.Equals("when_opt")) {
				WhenExpression = node.FindNode<IExpressionNode>();
			}

			return base.OnChildNode(node);
		}
	}
}
