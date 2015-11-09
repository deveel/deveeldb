using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class PrivilegeNode : SqlNode {
		static PrivilegeNode() {
			All = new PrivilegeNode {Privilege = "ALL"};
		}

		public string Privilege { get; private set; }

		public IEnumerable<string> Columns { get; private set; }

		public static PrivilegeNode All { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {
				Privilege = ((SqlKeyNode) node).Text;
			} else if (node.NodeName.Equals("update_priv")) {
				Privilege = "UPDATE";
				GetPrivilegeWithColumns(node);
			} else if (node.NodeName.Equals("select_priv")) {
				Privilege = "SELECT";
				GetPrivilegeWithColumns(node);
			} else if (node.NodeName.Equals("reference_priv")) {
				Privilege = "REFERENCE";
				GetPrivilegeWithColumns(node);
			}

			return base.OnChildNode(node);
		}

		private void GetPrivilegeWithColumns(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName.Equals("column_list_opt"))
					GetColumns(childNode);
			}
		}

		private void GetColumns(ISqlNode node) {
			var childNode = node.ChildNodes.FirstOrDefault();
			if (childNode == null)
				return;

			var list = new List<string>();
			foreach (var idNode in childNode.ChildNodes) {
				if (idNode is IdentifierNode)
					list.Add(((IdentifierNode)idNode).Text);
			}

			Columns = list.AsEnumerable();
		}
	}
}
