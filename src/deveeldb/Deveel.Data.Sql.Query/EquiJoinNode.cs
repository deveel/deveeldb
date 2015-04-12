using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class EquiJoinNode : BranchQueryPlanNode {
		public EquiJoinNode(IQueryPlanNode left, IQueryPlanNode right, ObjectName[] leftColumns, ObjectName[] rightColumns) 
			: base(left, right) {
			LeftColumns = leftColumns;
			RightColumns = rightColumns;
		}

		public ObjectName[] LeftColumns { get; private set; }

		public ObjectName[] RightColumns { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			// Solve the left branch result
			var leftResult = Left.Evaluate(context);
			// Solve the right branch result
			var rightResult = Right.Evaluate(context);

			// TODO: This needs to migrate to a better implementation that
			//   exploits multi-column indexes if one is defined that can be used.

			var firstLeft = SqlExpression.Reference(LeftColumns[0]);
			var firstRight = SqlExpression.Reference(RightColumns[0]);
			var onExpression = SqlExpression.Equal(firstLeft, firstRight);

			var result = leftResult.SimpleJoin(context, rightResult, onExpression);

			int sz = LeftColumns.Length;

			// If there are columns left to equi-join, we resolve the rest with a
			// single exhaustive select of the form,
			//   ( table1.col2 = table2.col2 AND table1.col3 = table2.col3 AND ... )
			if (sz > 1) {
				// Form the expression
				SqlExpression restExpression = null;
				for (int i = 1; i < sz; ++i) {
					var left = SqlExpression.Reference(LeftColumns[i]);
					var right = SqlExpression.Reference(RightColumns[i]);
					var equalExp = SqlExpression.And(left, right);

					if (restExpression == null) {
						restExpression = equalExp;
					} else {
						restExpression = SqlExpression.And(restExpression, equalExp);
					}
				}

				result = result.ExhaustiveSelect(context, restExpression);
			}

			return result;
		}
	}
}
