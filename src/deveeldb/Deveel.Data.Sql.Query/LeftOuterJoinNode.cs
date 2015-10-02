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
	/// <summary>
	/// A branch node for a left outer join.
	/// </summary>
	/// <remarks>
	/// Using this node is a little non-intuitive. This node will only 
	/// work when used in conjuction with <see cref="MarkerNode"/>.
	/// <para>
	/// To use - first the complete left table in the join must be marked 
	/// with a name. Then the <i>on expression</i> is evaluated to a single 
	/// plan node. Then this plan node must be added to result in a left 
	/// outer join. 
	/// A tree for a left outer join may look as follows:
	/// <code>
	///          LeftOuterJoinNode
	///                  |
	///              Join a = b
	///              /         \
	///          Marker      GetTable T2
	///            |
	///       GetTable T1
	/// </code>
	/// </para>
	/// </remarks>
	class LeftOuterJoinNode : SingleQueryPlanNode {
		public LeftOuterJoinNode(IQueryPlanNode child, string markerName) 
			: base(child) {
			MarkerName = markerName;
		}

		public string MarkerName { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			// Evaluate the child branch,
			var result = Child.Evaluate(context);
			// Get the table of the complete mark name,
			var completeLeft = context.GetCachedTable(MarkerName);

			// The rows in 'complete_left' that are outside (not in) the rows in the
			// left result.
			var outside = completeLeft.OuterJoin(result);

			// Create an OuterTable
			var outerTable = OuterTable.Create(result);
			outerTable.MergeIn(outside);

			// Return the outer table
			return outerTable;
		}
	}
}