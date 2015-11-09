using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class GrantStatementNode : SqlNode, IStatementNode {
		public string ObjectName { get; private set; }

		public IEnumerable<PrivilegeNode> Privileges { get; private set; } 
		
		public IEnumerable<string> Grantees { get; private set; }
		
		public bool WithGrant { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				ObjectName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName.Equals("object_priv")) {
				GetPrivileges(node);
			} else if (node.NodeName.Equals("distribution_list")) {
				GetGrantees(node);
			} else if (node.NodeName.Equals("with_grant_opt")) {
				GetWithGrant(node);
			}

			return base.OnChildNode(node);
		}

		private void GetWithGrant(ISqlNode node) {
			if (node.ChildNodes.Any())
				WithGrant = true;
		}

		private void GetGrantees(ISqlNode node) {
			Grantees = node.ChildNodes.OfType<IdentifierNode>().Select(x => x.Text);
		}

		private void GetPrivileges(ISqlNode node) {
			bool isAll = false;

			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					if (((SqlKeyNode) childNode).Text.Equals("ALL", StringComparison.OrdinalIgnoreCase)) {
						isAll = true;
					} else if (!((SqlKeyNode) childNode).Text.Equals("PRIVILEGES", StringComparison.OrdinalIgnoreCase)) {
						throw new InvalidOperationException();
					}
				} else if (childNode.NodeName.Equals("priv_list")) {
					Privileges = childNode.ChildNodes.OfType<PrivilegeNode>();
				}
			}

			if (isAll)
				Privileges = new[] {PrivilegeNode.All};
		}
	}
}
