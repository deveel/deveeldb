using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

using Deveel.Data.Util;

namespace Deveel.Data.Linq.Expressions {
	public sealed class InExpression : SubqueryExpression {
		public InExpression(Expression expression, SelectExpression query)
			: base(QueryExpressionType.In, typeof(bool), query) {
			Expression = expression;
		}

		public InExpression(Expression expression, IEnumerable<Expression> values)
			: base(QueryExpressionType.In, typeof(bool), null) {
			Values = values.ToReadOnly();
		}

		public Expression Expression { get; private set; }

		public ReadOnlyCollection<Expression> Values { get; private set; }
	}
}
