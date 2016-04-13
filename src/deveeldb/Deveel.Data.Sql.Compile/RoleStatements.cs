using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class RoleStatements {
		public static SqlStatement Drop(PlSqlParser.DropRoleStatementContext context) {
			var roleName = context.regular_id().GetText();
			return new DropRoleStatement(roleName);
		}

		public static SqlStatement Create(PlSqlParser.CreateRoleStatementContext context) {
			var roleName = context.regular_id().GetText();
			return new CreateRoleStatement(roleName);
		}
	}
}
