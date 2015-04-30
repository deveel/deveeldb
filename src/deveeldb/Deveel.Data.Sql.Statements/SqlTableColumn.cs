using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SqlTableColumn : IPreparable {
		public SqlTableColumn(string columnName, DataType columnType) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");
			if (columnType == null)
				throw new ArgumentNullException("columnType");
			
			ColumnName = columnName;
			ColumnType = columnType;
		}

		public string ColumnName { get; private set; }

		public DataType ColumnType { get; private set; }

		public bool IsIdentity { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		public bool HasDefaultExpression {
			get { return DefaultExpression != null; }
		}

		public bool IsNotNull { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var column = new SqlTableColumn(ColumnName, ColumnType);
			if (DefaultExpression != null)
				column.DefaultExpression = DefaultExpression.Prepare(preparer);

			column.IsNotNull = IsNotNull;
			return column;
		}
	}
}
