using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements.Build {
	class SelectStatementBuilder : ISelectStatementBuilder, IStatementBuilder {
		private SqlQueryExpression queryExpression;
		private List<SortColumn> sortColumns;
		private QueryLimit queryLimit;

		public SelectStatementBuilder() {
			sortColumns = new List<SortColumn>();
		}

		public ISelectStatementBuilder Query(SqlQueryExpression expression) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			queryExpression = expression;
			return this;
		}

		public ISelectStatementBuilder OrderBy(SortColumn sort) {
			if (sort != null)
				sortColumns.Add(sort);

			return this;
		}

		public ISelectStatementBuilder Limit(QueryLimit limit) {
			queryLimit = limit;
			return this;
		}

		public IEnumerable<SqlStatement> Build() {
			if (queryExpression == null)
				throw new InvalidOperationException("The query expression is required");

			var statement = new SelectStatement(queryExpression);

			if (sortColumns.Count > 0)
				statement.OrderBy = sortColumns.AsReadOnly();

			if (queryLimit != null)
				statement.Limit = queryLimit;

			return new[] {statement};
		}
	}
}
