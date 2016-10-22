using System;

namespace Deveel.Data.Sql.Tables {
	public static class ColumnInfoBuilderExtensions {
		public static IColumnInfoBuilder Null(this IColumnInfoBuilder builder, bool value = true) {
			return builder.NotNull(!value);
		}
	}
}
