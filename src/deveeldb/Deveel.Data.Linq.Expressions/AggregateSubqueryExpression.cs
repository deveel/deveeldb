using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class AggregateSubqueryExpression : QueryExpression {
		public AggregateSubqueryExpression(Alias groupBy, Expression inGroupSelect, ScalarExpression asSubquery)
			: base(QueryExpressionType.AggregateSubquery, asSubquery.Type) {
			GroupBy = groupBy;
			InGroupSelect = inGroupSelect;
			AsSubquery = asSubquery;
		}

		public Alias GroupBy { get; private set; }

		public Expression InGroupSelect { get; private set; }

		public ScalarExpression AsSubquery { get; private set; }
	}
}
