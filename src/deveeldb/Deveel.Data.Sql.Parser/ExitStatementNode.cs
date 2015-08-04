using System;

namespace Deveel.Data.Sql.Parser {
	class ExitStatementNode : SqlNode, IStatementNode {
		public string Label { get; private set; }

		public IExpressionNode ExitCondition { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("label_opt")) {
				Label = node.FindNode<IdentifierNode>().Text;
			} else if (node.NodeName.Equals("exit_when_opt")) {
				ExitCondition = node.FindNode<IExpressionNode>();
			}

			return base.OnChildNode(node);
		}
	}
}
