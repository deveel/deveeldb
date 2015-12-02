using System;

namespace Deveel.Data.Sql.Parser {
	class CreateSchemaNode : SqlNode, IStatementNode {
		public string SchemaName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode)
				SchemaName = ((IdentifierNode) node).Text;

			return base.OnChildNode(node);
		}
	}
}
