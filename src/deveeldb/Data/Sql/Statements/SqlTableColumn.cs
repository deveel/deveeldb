using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlTableColumn : ISqlExpressionPreparable<SqlTableColumn> {
		public SqlTableColumn(string columnName, SqlType columnType) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException(nameof(columnName));
			if (columnType == null)
				throw new ArgumentNullException(nameof(columnType));
			
			ColumnName = columnName;
			ColumnType = columnType;
		}

		public string ColumnName { get; }

		public SqlType ColumnType { get; }

		public bool IsIdentity { get; set; }

		public SqlExpression DefaultExpression { get; set; }


        SqlTableColumn ISqlExpressionPreparable<SqlTableColumn>.Prepare(ISqlExpressionPreparer preparer) {
	        var column = new SqlTableColumn(ColumnName, ColumnType);
	        if (DefaultExpression != null)
		        column.DefaultExpression = DefaultExpression.Prepare(preparer);

	        column.IsIdentity = IsIdentity;
	        return column;
        }
	}
}