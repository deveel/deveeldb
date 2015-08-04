using System;

namespace Deveel.Data.Sql.Parser {
	class LabelNode : SqlNode {
		public string Text { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode)
				Text = ((IdentifierNode) node).Text;

			return base.OnChildNode(node);
		}
	}
}
