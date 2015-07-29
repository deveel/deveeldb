using System;

namespace Deveel.Data.Sql.Parser {
	class DropCoulmnNode : SqlNode, IAlterActionNode {
		public string ColumnName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode)
				ColumnName = ((IdentifierNode) node).Text;

			return base.OnChildNode(node);
		}
	}
}
