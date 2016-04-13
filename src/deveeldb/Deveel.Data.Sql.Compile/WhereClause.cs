using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	class WhereClause {
		public SqlExpression Expression { get; set; }

		public string CurrentOf { get; set; }

		public static WhereClause Form(PlSqlParser.WhereClauseContext context) {
			if (context.current_of_clause() != null) {
				return new WhereClause {CurrentOf = context.current_of_clause().cursor_name().GetText()};
			}

			return new WhereClause {
				Expression = new SqlExpressionVisitor().Visit(context.condition_wrapper())
			};
		}
	}
}
