using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class ShowStatementNode : SqlStatementNode {
		public string Target { get; private set; }

		public string TableName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("target")) {
				GetShow(node);
			}

			return base.OnChildNode(node);
		}

		private void GetShow(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					var keyNode = (SqlKeyNode) childNode;
					Target = keyNode.Text;
				} else if (childNode is ObjectNameNode) {
					TableName = ((ObjectNameNode) childNode).Name;
				}
			}
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var target = ParseTarget(Target);
			var statement = new ShowStatement(target);

			if (!String.IsNullOrEmpty(TableName))
				statement.TableName = ObjectName.Parse(TableName);

			builder.AddObject(statement);
		}

		private static ShowTarget ParseTarget(string s) {
			if (String.Equals(s, "OPEN SESSIONS", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(s, "SESSIONS", StringComparison.OrdinalIgnoreCase))
				return ShowTarget.OpenSessions;
			if (String.Equals(s, "TABLES", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(s, "SCHEMA TABLES", StringComparison.OrdinalIgnoreCase))
				return ShowTarget.SchemaTables;

			try {
				return (ShowTarget) Enum.Parse(typeof (ShowTarget), s, true);
			} catch (Exception ex) {
				throw new SqlParseException("Invalid SHOW target type.", ex);
			}
		}
	}
}
