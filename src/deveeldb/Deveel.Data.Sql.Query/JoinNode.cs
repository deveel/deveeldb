using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class JoinNode : BranchQueryPlanNode {
		public JoinNode(IQueryPlanNode left, IQueryPlanNode right, ObjectName leftColumnName, SqlExpressionType @operator, SqlExpression rightExpression) 
			: base(left, right) {
			LeftColumnName = leftColumnName;
			Operator = @operator;
			RightExpression = rightExpression;
		}

		public ObjectName LeftColumnName { get; private set; }

		public SqlExpressionType Operator { get; private set; }

		public SqlExpression RightExpression { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			// Solve the left branch result
			var leftResult = Left.Evaluate(context);
			// Solve the right branch result
			var rightResult = Right.Evaluate(context);

			var rightExpression = RightExpression;

			// If the rightExpression is a simple variable then we have the option
			// of optimizing this join by putting the smallest table on the LHS.
			var rhsVar = rightExpression.AsReferenceName();
			var lhsVar = LeftColumnName;
			var op = Operator;

			if (rhsVar != null) {
				// We should arrange the expression so the right table is the smallest
				// of the sides.
				// If the left result is less than the right result

				if (leftResult.RowCount < rightResult.RowCount) {
					// Reverse the join
					rightExpression = SqlExpression.Reference(lhsVar);
					lhsVar = rhsVar;
					op = op.Reverse();

					// Reverse the tables.
					var t = rightResult;
					rightResult = leftResult;
					leftResult = t;
				}
			}

			var joinExp = SqlExpression.Binary(SqlExpression.Reference(lhsVar), op, rightExpression);

			// The join operation.
			return leftResult.SimpleJoin(context, rightResult, joinExp);
		}
	}
}
