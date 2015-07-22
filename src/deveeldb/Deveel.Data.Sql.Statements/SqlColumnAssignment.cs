using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlColumnAssignment {
		public SqlColumnAssignment(string columnName, SqlExpression expression) {
			if (expression == null)
				throw new ArgumentNullException("expression");
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			ColumnName = columnName;
			Expression = expression;
		}

		public string ColumnName { get; private set; }

		public SqlExpression Expression { get; private set; }
	}
}
