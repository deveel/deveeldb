using System;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class DeleteBuilder {
		public static SqlStatement Build(PlSqlParser.DeleteStatementContext context) {
			var tableName = Name.Object(context.objectName());
			var whereClause = WhereClause.Form(context.whereClause());

			if (whereClause.CurrentOf != null)
				return new DeleteCurrentStatement(tableName, whereClause.CurrentOf);

			var statement = new DeleteStatement(tableName, whereClause.Expression);

			if (context.delete_limit() != null) {
				var limit = Number.PositiveInteger(context.delete_limit().numeric());
				if (limit == null)
					throw new ParseCanceledException("Invalid delete limit.");

				statement.Limit = limit.Value;
			}

			return statement;
		}
	}
}
