using System;

namespace Deveel.Data.Sql.Parser {
	class DataTypeMetaNode : SqlNode {
		public string Name { get; private set; }

		public string Value { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				Name = ((IdentifierNode) node).Text;
			} else if (node is StringLiteralNode) {
				Value = ((StringLiteralNode) node).Value;
			}

			return base.OnChildNode(node);
		}
	}
}
