using System;

namespace Deveel.Data.Sql.Parser {
	class SetDefaultNode : SqlNode, IAlterActionNode {
		public string ColumnName { get; private set; }

		public IExpressionNode Expression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				ColumnName = ((IdentifierNode) node).Text;
			} else if (node is IExpressionNode) {
				Expression = (IExpressionNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}
