using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class SimplePatternSelectNode : SingleQueryPlanNode {
		public SimplePatternSelectNode(IQueryPlanNode child, SqlExpression expression) 
			: base(child) {
			Expression = expression;
		}

		public SqlExpression Expression { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var t = Child.Evaluate(context);

			if (Expression is SqlBinaryExpression) {
				var binary = (SqlBinaryExpression) Expression;

				// Perform the pattern search expression on the table.
				// Split the expression,
				var leftRef = binary.Left.AsReferenceName();
				if (leftRef != null)
					// LHS is a simple variable so do a simple select
					return t.SimpleSelect(context, leftRef, binary.ExpressionType, binary.Right);
			}

			// LHS must be a constant so we can just evaluate the expression
			// and see if we get true, false, null, etc.
			var v = Expression.EvaluateToConstant(context, null);

			// If it evaluates to NULL or FALSE then return an empty set
			if (v.IsNull || v == false)
				return t.EmptySelect();

			return t;
		}
	}
}
