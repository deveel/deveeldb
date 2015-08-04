using System;

namespace Deveel.Data.Sql.Parser {
	class GotoStatementNode : SqlNode, IStatementNode {
		public string Label { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode)
				Label = ((IdentifierNode) node).Text;

			return base.OnChildNode(node);
		}
	}
}
