using System;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	class ColumnConstraintNode : SqlNode {
		private bool notSeen;

		public string ConstraintType { get; private set; }

		public IExpressionNode CheckExpression { get; private set; }

		public ObjectNameNode ReferencedTable { get; private set; }

		public IdentifierNode ReferencedColumn { get; private set; }

		public string OnDeleteAction { get; private set; }

		public string OnUpdateAction { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {
				var keyNode = (SqlKeyNode) node;
				if (String.Equals(keyNode.Text, "NULL", StringComparison.OrdinalIgnoreCase)) {
					if (notSeen) {
						ConstraintType = "NOT NULL";
					} else {
						ConstraintType = "NULL";
					}
				} else if (String.Equals(keyNode.Text, "NOT", StringComparison.OrdinalIgnoreCase)) {
					notSeen = true;
				} else if (String.Equals(keyNode.Text, "REFERENCES", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.ForeignKey;
				} else if (String.Equals(keyNode.Text, "CHECK", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.Check;
				} else if (String.Equals(keyNode.Text, "PRIMARY", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.PrimaryKey;
				} else if (String.Equals(keyNode.Text, "UNIQUE", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.UniqueKey;
				}
			} else if (node is IExpressionNode) {
				CheckExpression = (IExpressionNode) node;
			} else if (node is ObjectNameNode) {
				if (!String.Equals(ConstraintType, ConstraintTypeNames.ForeignKey, StringComparison.OrdinalIgnoreCase))
					throw new InvalidOperationException();

				ReferencedTable = ((ObjectNameNode) node);
			}

			return base.OnChildNode(node);
		}
	}
}
