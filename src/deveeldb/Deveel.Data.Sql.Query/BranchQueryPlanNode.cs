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
using System.Collections.Generic;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> implementation that is a branch with 
	/// two child nodes.
	/// </summary>
	[Serializable]
	public abstract class BranchQueryPlanNode : QueryPlanNode {
		// The left and right node.

		protected BranchQueryPlanNode(QueryPlanNode left, QueryPlanNode right) {
			Left = left;
			Right = right;
		}

		/// <summary>
		/// Gets the left node of the branch query plan node.
		/// </summary>
		protected QueryPlanNode Left { get; private set; }

		/// <summary>
		/// Gets the right node of the branch query plan node.
		/// </summary>
		protected QueryPlanNode Right { get; private set; }

		/// <inheritdoc/>
		internal override IList<ObjectName> DiscoverTableNames(IList<ObjectName> list) {
			return Right.DiscoverTableNames(Left.DiscoverTableNames(list));
		}

		/// <inheritdoc/>
		internal override IList<QueryReference> DiscoverQueryReferences(int level, IList<QueryReference> list) {
			return Right.DiscoverQueryReferences(level, Left.DiscoverQueryReferences(level, list));
		}

		public virtual string NodeName {
			get { return GetType().Name; }
		}
	}
}