using System;

namespace Deveel.Data.Sql.Parser {
	class DeclarePragmaNode : SqlNode, IDeclareNode {
		public string ExceptionName { get; private set; }

		public long ErrorNumber { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is StringLiteralNode) {
				ExceptionName = ((StringLiteralNode) node).Value;
			} else if (node is IntegerLiteralNode) {
				ErrorNumber = ((IntegerLiteralNode) node).Value;
			}

			return base.OnChildNode(node);
		}
	}
}
