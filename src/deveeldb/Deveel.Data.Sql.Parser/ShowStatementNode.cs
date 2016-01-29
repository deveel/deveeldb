using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class ShowStatementNode : SqlStatementNode {
		public string Target { get; private set; }

		public string TableName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			return base.OnChildNode(node);
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

			try {
				return (ShowTarget) Enum.Parse(typeof (ShowTarget), s, true);
			} catch (Exception ex) {
				throw new SqlParseException("Invalid SHOW target type.", ex);
			}
		}
	}
}
