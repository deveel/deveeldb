using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class JoinExpression : QueryExpression {
		public JoinExpression(Expression left, JoinType joinType, Expression right, Expression condition)
			: base(QueryExpressionType.Join, typeof(void)) {
			Left = left;
			JoinType = joinType;
			Right = right;
			Condition = condition;
		}

		public Expression Left { get; private set; }

		public JoinType JoinType { get; private set; }

		public Expression Right { get; private set; }

		public Expression Condition { get; private set; }
	}
}
