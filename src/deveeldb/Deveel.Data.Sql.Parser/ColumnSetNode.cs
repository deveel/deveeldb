using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class InsertSetNode : SqlNode {
		public IExpressionNode Value { get; private set; }

		public string ColumnName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				Value = (IExpressionNode) node;
			} else if (node is IdentifierNode) {
				ColumnName = ((IdentifierNode) node).Text;
			}

			return base.OnChildNode(node);
		}
	}
}
