using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Linq {
	class ColumnQuery {
		public ColumnQuery(string columnName, SqlExpression expression) {
			ColumnName = columnName;
			Expression = expression;
		}

		public string ColumnName { get; private set; }

		public SqlExpression Expression { get; private set; }
	}
}
