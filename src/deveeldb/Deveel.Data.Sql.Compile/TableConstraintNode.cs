using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;

using Deveel.Data.Sql.Expressions.Fluid;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	class TableConstraintNode : SqlNode {
		private bool notSeen;
		private readonly IList<string> columns;
		private readonly IList<string> refColumns;

		public TableConstraintNode() {
			columns = new List<string>();
			refColumns = new List<string>();
		}

		public string ConstraintName { get; private set; }

		public string ConstraintType { get; private set; }

		public IEnumerable<string> Columns {
			get { return columns.AsEnumerable(); }
		}

		public IExpressionNode CheckExpression { get; private set; }

		public ObjectNameNode ReferencedTableName { get; private set; }

		public IEnumerable<string> ReferencedColumns {
			get { return refColumns.AsEnumerable(); }
		}

		public string OnUpdateAction { get; private set; }

		public string OnDeleteAction { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "table_constraint_name_opt") {
				ReadConstraintName(node.ChildNodes);
			} else if (node.NodeName == "def_table_constraint") {
				ReadConstraintDefinition(node);
			}

			return base.OnChildNode(node);
		}

		private void ReadConstraintDefinition(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					var keyNode = (SqlKeyNode)childNode;
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
				} else if (childNode.NodeName == "column_list") {
					ReadColumnList(childNode.ChildNodes);
				}				
			}
		}

		private void ReadConstraintName(IEnumerable<ISqlNode> nodes) {
			foreach (var node in nodes) {
				if (node is IdentifierNode) {
					ConstraintName = ((IdentifierNode) node).Text;
				} else {
					ReadConstraintName(node.ChildNodes);
				}
			}
		}

		private void ReadColumnList(IEnumerable<ISqlNode> nodes) {
			foreach (var node in nodes) {
				if (node is IdentifierNode) {
					columns.Add(((IdentifierNode)node).Text);
				} else {
					ReadColumnList(node.ChildNodes);
				}
			}
		}
	}
}
