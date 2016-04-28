using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Util;

namespace Deveel.Data.Linq.Expressions {
	public sealed class SelectExpression : AliasedExpression {
		public SelectExpression(IEnumerable<QueryColumn> columns, Expression from, Expression where, Alias alias)
			: this(columns, @from, @where, null, null, alias) {
		}

		public SelectExpression(IEnumerable<QueryColumn> columns, Expression from, Expression where, IEnumerable<OrderExpression> orderBy, IEnumerable<Expression> groupBy, Alias alias)
			: this(false, columns, @from, @where, orderBy, groupBy, null, null, alias) {
		}

		public SelectExpression(bool distinct, IEnumerable<QueryColumn> columns, Expression from, Expression where, IEnumerable<OrderExpression> orderBy, IEnumerable<Expression> groupBy, Expression skip, Expression take, Alias alias)
			: base(QueryExpressionType.Select, typeof(void), alias) {
			Columns = columns.ToReadOnly();
			Distinct = distinct;
			From = from;
			Where = where;
			Skip = skip;
			Take = take;
			OrderBy = orderBy.ToReadOnly();
			GroupBy = groupBy.ToReadOnly();
		}

		public ReadOnlyCollection<QueryColumn> Columns { get; private set; }

		public bool Distinct { get; private set; }

		public Expression From { get; private set; }

		public Expression Where { get; private set; }

		public Expression Skip { get; private set; }

		public Expression Take { get; private set; }

		public ReadOnlyCollection<OrderExpression> OrderBy { get; private set; }

		public ReadOnlyCollection<Expression> GroupBy { get; private set; }
	}
}
