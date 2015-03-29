using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	class TableColumnNode : SqlNode {
		private readonly ICollection<ColumnConstraintNode> constraints;

		public TableColumnNode() {
			constraints = new List<ColumnConstraintNode>();
		}

		public string ColumnName { get; private set; }

		public DataTypeNode DataType { get; private set; }

		public IEnumerable<ColumnConstraintNode> Constraints {
			get { return constraints.AsEnumerable(); }
		}

		public IExpressionNode Default { get; private set; }

		public bool IsIdentity { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "column_name") {
				ColumnName = ((IdentifierNode) node.ChildNodes.First()).Text;
			} else if (node is DataTypeNode) {
				DataType = (DataTypeNode) node;
			} else if (node.NodeName == "column_constraint_list") {
				ReadColumnConstraints(node.ChildNodes);
			} else if (node.NodeName == "column_default_opt") {
				ReadDefaultExpression(node.ChildNodes);
			}

			return base.OnChildNode(node);
		}

		private void ReadDefaultExpression(IEnumerable<ISqlNode> nodes) {
			foreach (var child in nodes) {
				if (child is IExpressionNode) {
					Default = (IExpressionNode) child;
				} else {
					ReadDefaultExpression(child.ChildNodes);
				}
			}
		}

		private void ReadColumnConstraints(IEnumerable<ISqlNode> childNodes) {
			foreach (var childNode in childNodes) {
				if (childNode is ColumnConstraintNode) {
					constraints.Add(childNode as ColumnConstraintNode);
				} else {
					ReadColumnConstraints(childNode.ChildNodes);
				}
			}
		}
	}
}
