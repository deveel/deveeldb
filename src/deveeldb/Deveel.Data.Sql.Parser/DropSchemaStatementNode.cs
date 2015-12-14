using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DropSchemaStatementNode : SqlStatementNode {
		public string SchemaName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				SchemaName = ((IdentifierNode) node).Text;
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(StatementBuilder builder) {
			builder.Statements.Add(new DropSchemaStatement(SchemaName));
		}
	}
}