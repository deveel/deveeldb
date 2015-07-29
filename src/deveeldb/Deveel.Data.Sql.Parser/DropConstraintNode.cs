using System;

namespace Deveel.Data.Sql.Parser {
	class DropConstraintNode : SqlNode, IAlterActionNode {
		public string ConstraintName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode)
				ConstraintName = ((IdentifierNode) node).Text;

			return base.OnChildNode(node);
		}
	}
}
