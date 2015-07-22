using System;

namespace Deveel.Data.Sql.Parser {
	class UpdateColumnNode : SqlNode {
		internal UpdateColumnNode() {
		}

		public string ColumnName { get; private set; }

		public IExpressionNode Expression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				ColumnName = ((ObjectNameNode) node).Name;
			} else if (node is IdentifierNode) {
				ColumnName = ((IdentifierNode) node).Text;
			} else if (node is IExpressionNode) {
				Expression = (IExpressionNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}
