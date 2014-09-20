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
using System.Collections.Generic;
using System.Text;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> with a single child.
	/// </summary>
	[Serializable]
	public abstract class SingleQueryPlanNode : IQueryPlanNode {
		/// <summary>
		/// The single child node.
		/// </summary>
		private IQueryPlanNode child;

		protected SingleQueryPlanNode(IQueryPlanNode child) {
			this.child = child;
		}

		/// <summary>
		/// Gets the single child node of the plan.
		/// </summary>
		protected IQueryPlanNode Child {
			get { return child; }
		}

		/// <inheritdoc/>
		public abstract Table Evaluate(IQueryContext context);

		/// <inheritdoc/>
		public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return child.DiscoverTableNames(list);
		}

		/// <inheritdoc/>
		public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return child.DiscoverCorrelatedVariables(level, list);
		}

		/// <inheritdoc/>
		public virtual Object Clone() {
			SingleQueryPlanNode node = (SingleQueryPlanNode)MemberwiseClone();
			node.child = (IQueryPlanNode)child.Clone();
			return node;
		}

		public virtual string Title {
			get { return GetType().Name; }
		}

		/// <inheritdoc/>
		public void DebugString(int level, StringBuilder sb) {
			QueryPlanUtil.Indent(level, sb);
			sb.Append(Title);
			sb.Append('\n');
			child.DebugString(level + 2, sb);
		}
	}
}