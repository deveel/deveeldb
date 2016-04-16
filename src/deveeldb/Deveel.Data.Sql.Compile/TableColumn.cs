using System;
using System.Collections.Generic;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
	static class TableColumn {
		public static SqlTableColumn Form(PlSqlParser.TableColumnContext context, List<ColumnConstraint> constraints) {

			var columnName = Name.Simple(context.columnName());
			var columnType = SqlTypeParser.Parse(context.datatype());

			if (columnType == null)
				throw new ParseCanceledException("No type was found for table.");

			SqlExpression defaultExpression = null;
			bool identity = false;
			bool nullable = true;

			if (context.IDENTITY() != null) {
				if (!(columnType is NumericType))
					throw new InvalidOperationException("Cannot have an identity column that has not a numeric type");

				identity = true;
			} else {
				var columnConstraints = context.columnConstraint();
				if (columnConstraints != null &&
				    columnConstraints.Length > 0) {
					foreach (var constraintContext in columnConstraints) {
						if (constraintContext.PRIMARY() != null) {
							constraints.Add(new ColumnConstraint {
								ColumnName = columnName,
								Type = ConstraintType.PrimaryKey
							});
						} else if (constraintContext.UNIQUE() != null) {
							constraints.Add(new ColumnConstraint {
								Type = ConstraintType.Unique,
								ColumnName = columnName
							});
						} else if (constraintContext.NOT() != null &&
						           constraintContext.NULL() != null) {
							nullable = false;
						}
					}
				}

				if (context.defaultValuePart() != null) {
					defaultExpression = Expression.Build(context.defaultValuePart().expression());
				}
			}

			return new SqlTableColumn(columnName, columnType) {
				IsNotNull = !nullable,
				IsIdentity = identity,
				DefaultExpression = defaultExpression
			};
		}
	}
}