using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class RevokeStatementNode : SqlStatementNode {
		public string ObjectName { get; private set; }

		public IEnumerable<PrivilegeNode> Privileges { get; private set; }

		public IEnumerable<string> Grantees { get; private set; }

		public bool GrantOption { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				ObjectName = ((ObjectNameNode)node).Name;
			} else if (node.NodeName.Equals("object_priv")) {
				GetPrivileges(node);
			} else if (node.NodeName.Equals("distribution_list")) {
				GetGrantees(node);
			} else if (node.NodeName.Equals("grant_opt")) {
				GetGrantOption(node);
			}

			return base.OnChildNode(node);
		}

		private void GetGrantOption(ISqlNode node) {
			if (node.ChildNodes.Any())
				GrantOption = true;
		}

		private void GetGrantees(ISqlNode node) {
			Grantees = node.ChildNodes.OfType<IdentifierNode>().Select(x => x.Text);
		}

		private void GetPrivileges(ISqlNode node) {
			bool isAll = false;

			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					if (((SqlKeyNode)childNode).Text.Equals("ALL", StringComparison.OrdinalIgnoreCase)) {
						isAll = true;
					} else if (!((SqlKeyNode)childNode).Text.Equals("PRIVILEGES", StringComparison.OrdinalIgnoreCase)) {
						throw new InvalidOperationException();
					}
				} else if (childNode.NodeName.Equals("priv_list")) {
					Privileges = childNode.ChildNodes.OfType<PrivilegeNode>();
				}
			}

			if (isAll)
				Privileges = new[] { PrivilegeNode.All };
		}

		private static Privileges ParsePrivilege(string privName) {
			try {
				return (Privileges)Enum.Parse(typeof(Privileges), privName, true);
			} catch (Exception) {
				throw new InvalidOperationException(String.Format("Invalid privilege name '{0}' specified.", privName));
			}
		}


		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var objName = Sql.ObjectName.Parse(ObjectName);
			foreach (var grantee in Grantees) {
				var grantPrivilege = Security.Privileges.None;
				var columns = new string[0];

				foreach (var privilegeNode in Privileges) {
					var privilege = ParsePrivilege(privilegeNode.Privilege);
					grantPrivilege |= privilege;

					if (privilegeNode.Columns != null) {
						var privCols = new List<string>(columns);
						foreach (var privColumn in privilegeNode.Columns) {
							if (!privCols.Contains(privColumn))
								privCols.Add(privColumn);
						}

						columns = privCols.ToArray();
					}
				}

				builder.AddObject(new RevokePrivilegesStatement(grantee, grantPrivilege, GrantOption, objName, columns));
			}
		}
	}
}
