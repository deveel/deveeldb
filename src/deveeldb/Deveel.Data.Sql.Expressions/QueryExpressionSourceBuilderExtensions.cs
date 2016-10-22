using System;

namespace Deveel.Data.Sql.Expressions {
	public static class QueryExpressionSourceBuilderExtensions {
		public static IQueryExpressionSourceBuilder Table(this IQueryExpressionSourceBuilder builder, string tableName) {
			return builder.Table(ObjectName.Parse(tableName));
		}
	}
}
