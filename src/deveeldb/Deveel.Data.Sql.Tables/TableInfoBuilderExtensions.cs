using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public static class TableInfoBuilderExtensions {
		public static ITableInfoBuilder Named(this ITableInfoBuilder builder, ObjectName tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			return builder.InSchema(tableName.ParentName).Named(tableName.Name);
		}

		public static ITableInfoBuilder WithColumn(this ITableInfoBuilder builder, string columnName, SqlType columnType) {
			return builder.WithColumn(column => column.Named(columnName).HavingType(columnType));
		}
	}
}
