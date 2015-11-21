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
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> implementation that is a branch with 
	/// two child nodes.
	/// </summary>
	abstract class BranchQueryPlanNode : IQueryPlanNode {
		// The left and right node.

		protected BranchQueryPlanNode(IQueryPlanNode left, IQueryPlanNode right) {
			Left = left;
			Right = right;
		}

		/// <summary>
		/// Gets the left node of the branch query plan node.
		/// </summary>
		public IQueryPlanNode Left { get; private set; }

		/// <summary>
		/// Gets the right node of the branch query plan node.
		/// </summary>
		public IQueryPlanNode Right { get; private set; }

		public abstract ITable Evaluate(IQueryContext context);
	}
}