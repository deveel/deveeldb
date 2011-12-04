// 
//  Copyright 2010  Deveel
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
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> implementation that is a branch with 
	/// two child nodes.
	/// </summary>
	[Serializable]
	public abstract class BranchQueryPlanNode : IQueryPlanNode {
		// The left and right node.
		private IQueryPlanNode left;
		private IQueryPlanNode right;

		protected BranchQueryPlanNode(IQueryPlanNode left, IQueryPlanNode right) {
			this.left = left;
			this.right = right;
		}

		/// <summary>
		/// Gets the left node of the branch query plan node.
		/// </summary>
		protected IQueryPlanNode Left {
			get { return left; }
		}

		/// <summary>
		/// Gets the right node of the branch query plan node.
		/// </summary>
		protected IQueryPlanNode Right {
			get { return right; }
		}

		/// <inheritdoc/>
		public abstract Table Evaluate(IQueryContext context);

		/// <inheritdoc/>
		public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return right.DiscoverTableNames(left.DiscoverTableNames(list));
		}

		/// <inheritdoc/>
		public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return right.DiscoverCorrelatedVariables(level, left.DiscoverCorrelatedVariables(level, list));
		}

		/// <inheritdoc/>
		public virtual Object Clone() {
			BranchQueryPlanNode node = (BranchQueryPlanNode)MemberwiseClone();
			node.left = (IQueryPlanNode)left.Clone();
			node.right = (IQueryPlanNode)right.Clone();
			return node;
		}

		public virtual string Title {
			get { return GetType().Name; }
		}

		/// <inheritdoc/>
		public virtual void DebugString(int level, StringBuilder sb) {
			QueryPlanUtil.Indent(level, sb);
			sb.Append(Title);
			sb.Append('\n');
			left.DebugString(level + 2, sb);
			right.DebugString(level + 2, sb);
		}

	}
}