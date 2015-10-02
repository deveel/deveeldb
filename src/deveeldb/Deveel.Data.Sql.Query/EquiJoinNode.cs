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

using Deveel.Data;

namespace Deveel.Data.Sql.Query {
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

			return leftResult.EquiJoin(context, rightResult, LeftColumns, RightColumns);
		}
	}
}
