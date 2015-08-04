using System;

namespace Deveel.Data.Sql.Parser {
	class DeclareExceptionNode : SqlNode, IDeclareNode {
		public string Name { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode)
				Name = ((IdentifierNode) node).Text;

			return base.OnChildNode(node);
		}
	}
}
