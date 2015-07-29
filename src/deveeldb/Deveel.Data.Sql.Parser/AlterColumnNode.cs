using System;

namespace Deveel.Data.Sql.Parser {
	class AlterColumnNode : SqlNode, IAlterActionNode {
		public TableColumnNode Column { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is TableColumnNode)
				Column = (TableColumnNode) node;

			return base.OnChildNode(node);
		}
	}
}
