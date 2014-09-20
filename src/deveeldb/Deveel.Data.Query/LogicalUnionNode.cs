// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// A branch node for a logical union of two tables of identical types.
	/// </summary>
	/// <remarks>
	/// This branch can only work if the left and right children have 
	/// exactly the same ancestor tables. If the ancestor tables are 
	/// different it will fail. This node is used for logical <b>or</b>.
	/// <para>
	/// This union does not include duplicated rows.
	/// </para>
	/// </remarks>
	[Serializable]
	public class LogicalUnionNode : BranchQueryPlanNode {
		public LogicalUnionNode(IQueryPlanNode left, IQueryPlanNode right)
			: base(left, right) {
		}

		public override Table Evaluate(IQueryContext context) {
			// Solve the left branch result
			Table leftResult = Left.Evaluate(context);
			// Solve the right branch result
			Table rightResult = Right.Evaluate(context);

			return leftResult.Union(rightResult);
		}

		public override string Title {
			get { return "LOGICAL Union"; }
		}
	}
}