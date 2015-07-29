using System;

namespace Deveel.Data.Sql.Parser {
	class AddConstraintNode : SqlNode, IAlterActionNode {
		public TableConstraintNode Constraint { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is TableConstraintNode)
				Constraint = (TableConstraintNode) node;

			return base.OnChildNode(node);
		}
	}
}
