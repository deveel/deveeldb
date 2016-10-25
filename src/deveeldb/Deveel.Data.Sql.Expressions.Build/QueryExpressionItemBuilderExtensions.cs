using System;

namespace Deveel.Data.Sql.Expressions.Build {
	public static class QueryExpressionItemBuilderExtensions {
		public static IQueryExpressionItemBuilder Expression(this IQueryExpressionItemBuilder builder,
			Action<IExpressionBuilder> expression) {
			var expBuilder = new ExpressionBuilder();
			expression(expBuilder);

			return builder.Expression(expBuilder.Build());
		}
	}
}
