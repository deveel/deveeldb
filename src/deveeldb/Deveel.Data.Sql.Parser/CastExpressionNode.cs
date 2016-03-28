using System;

namespace Deveel.Data.Sql.Parser {
	class CastExpressionNode : SqlNode, IExpressionNode {
		public IExpressionNode Expression { get; private set; }

		public DataTypeNode DataType { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				Expression = (IExpressionNode) node;
			} else if (node is DataTypeNode) {
				DataType = (DataTypeNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}
