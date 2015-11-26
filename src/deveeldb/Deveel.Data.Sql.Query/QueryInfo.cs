using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryInfo {
		public QueryInfo(IRequest request, SqlQueryExpression expression) 
			: this(request, expression, (QueryLimit) null) {
		}

		public QueryInfo(IRequest request, SqlQueryExpression expression, IEnumerable<SortColumn> sortColumns) 
			: this(request, expression, sortColumns, null) {
		}

		public QueryInfo(IRequest request, SqlQueryExpression expression, QueryLimit limit) : this(request, expression, null, limit) {
		}

		public QueryInfo(IRequest request, SqlQueryExpression expression, IEnumerable<SortColumn> sortColumns, QueryLimit limit) {
			if (expression == null)
				throw new ArgumentNullException("expression");
			if (request == null)
				throw new ArgumentNullException("request");

			Expression = expression;
			Request = request;
			SortColumns = sortColumns;
			Limit = limit;
		}

		public SqlQueryExpression Expression { get; private set; }

		public QueryLimit Limit { get; set; }

		public IRequest Request { get; private set; }

		public IEnumerable<SortColumn> SortColumns { get; set; }
	}
}
