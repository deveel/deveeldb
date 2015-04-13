// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

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
