using System;

namespace Deveel.Data.Sql.Parser {
	class SetPasswordNode : SqlNode, IAlterUserActionNode {
		public IExpressionNode Password { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode)
				Password = (IExpressionNode) node;

			return base.OnChildNode(node);
		}
	}
}
