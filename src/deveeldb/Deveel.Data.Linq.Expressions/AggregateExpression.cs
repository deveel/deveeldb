using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class AggregateExpression : QueryExpression {
		public AggregateExpression(string aggregateName, Expression argument, bool distinct, Type type)
			: base(QueryExpressionType.Aggregate, type) {
			AggregateName = aggregateName;
			Argument = argument;
			Distinct = distinct;
		}

		public string AggregateName { get; private set; }

		public Expression Argument { get; private set; }

		public bool Distinct { get; private set; }
	}
}
