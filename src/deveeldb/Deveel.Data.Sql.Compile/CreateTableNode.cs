using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	class CreateTableNode : SqlNode, IStatementNode {
		private readonly ICollection<TableColumnNode> columns;
		private readonly ICollection<TableConstraintNode> constraints;
 
		public CreateTableNode() {
			columns = new List<TableColumnNode>();
			constraints = new List<TableConstraintNode>();
		}

		public ObjectName TableName { get; private set; }

		public bool IfNotExists { get; private set; }

		public bool Temporary { get; private set; }

		public IEnumerable<TableColumnNode> Columns {
			get { return columns.AsEnumerable(); }
		}

		public IEnumerable<TableConstraintNode> Constraints {
			get { return constraints.AsEnumerable(); }
		}

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName == "if_not_exists_opt") {
				IfNotExists = true;
			} else if (node.NodeName == "temporary_opt") {
				Temporary = true;
			} else if (node.NodeName == "column_or_constraint_list") {
				ReadColumnOrConstraints(node.ChildNodes);
			}

			return base.OnChildNode(node);
		}

		private void ReadColumnOrConstraints(IEnumerable<ISqlNode> nodes) {
			foreach (var childNode in nodes) {
				if (childNode is TableColumnNode) {
					columns.Add(childNode as TableColumnNode);
				} else if (childNode is TableConstraintNode) {
					constraints.Add(childNode as TableConstraintNode);
				} else {
					ReadColumnOrConstraints(childNode.ChildNodes);
				}
			}
		}
	}
}
