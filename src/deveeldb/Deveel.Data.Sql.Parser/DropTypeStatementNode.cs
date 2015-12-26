using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DropTypeStatementNode : SqlStatementNode {
		public string TypeName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TypeName = ((ObjectNameNode) node).Name;
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (String.IsNullOrEmpty(TypeName))
				throw new InvalidOperationException();

			var typeName = ObjectName.Parse(TypeName);
			builder.Objects.Add(new DropTypeStatement(typeName));
		}
	}
}