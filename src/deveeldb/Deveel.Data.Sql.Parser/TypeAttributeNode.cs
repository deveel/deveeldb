using System;

namespace Deveel.Data.Sql.Parser {
	class TypeAttributeNode : SqlNode {
		public string Name { get; private set; }

		public DataTypeNode Type { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				Name = ((IdentifierNode) node).Text;
			} else if (node is DataTypeNode) {
				Type = ((DataTypeNode) node);
			}

			return base.OnChildNode(node);
		}
	}
}