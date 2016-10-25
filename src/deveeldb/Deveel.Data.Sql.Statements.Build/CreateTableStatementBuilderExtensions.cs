using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements.Build {
	public static class CreateTableStatementBuilderExtensions {
		public static ICreateTableStatementBuilder Named(this ICreateTableStatementBuilder builder, ObjectName parentName, string tableName) {
			return builder.Named(new ObjectName(parentName, tableName));
		}

		public static ICreateTableStatementBuilder Named(this ICreateTableStatementBuilder builder, string tableName) {
			return builder.Named(ObjectName.Parse(tableName));
		}

		public static ICreateTableStatementBuilder WithColumn(this ICreateTableStatementBuilder builder, string columnName, SqlType columnType) {
			return builder.WithColumn(column => column.Named(columnName).OfType(columnType));
		}

		public static ICreateTableStatementBuilder WithColumn(this ICreateTableStatementBuilder builder, Action<IColumnBuilder> column) {
			var columnBuilder = new ColumnBuilder();
			column(columnBuilder);

			ColumnConstraintInfo constraint;
			var columnResult = columnBuilder.Build(out constraint);

			if (constraint != null)
				builder.WithConstraint(new SqlTableConstraint(constraint.ConstraintType, new []{columnResult.ColumnName}) {
					ReferenceTable = constraint.ReferencedTable == null ? null : constraint.ReferencedTable.ToString(),
					ReferenceColumns = new []{constraint.ReferencedColumnName},
					OnUpdate = constraint.ActionOnUpdate,
					OnDelete = constraint.ActionOnDelete
				});

			return builder.WithColumn(columnResult);
		}

		public static ICreateTableStatementBuilder WithIdentityColumn(this ICreateTableStatementBuilder builder, string columnName, SqlType columnType) {
			return builder.WithColumn(column => column.Named(columnName).OfType(columnType).NotNull().Identity().PrimaryKey());
		}
	}
}
