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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> with a single child.
	/// </summary>
	[Serializable]
	public abstract class SingleQueryPlanNode : QueryPlanNode {
		/// <summary>
		/// The single child node.
		/// </summary>
		private QueryPlanNode child;

		protected SingleQueryPlanNode(QueryPlanNode child) {
			this.child = child;
		}

		/// <summary>
		/// Gets the single child node of the plan.
		/// </summary>
		protected IQueryPlanNode Child {
			get { return child; }
		}

		internal override IList<ObjectName> DiscoverTableNames(IList<ObjectName> list) {
			return child.DiscoverTableNames(list);
		}

		/// <inheritdoc/>
		internal override IList<QueryReference> DiscoverQueryReferences(int level, IList<QueryReference> list) {
			return child.DiscoverQueryReferences(level, list);
		}

		public virtual string Title {
			get { return GetType().Name; }
		}
	}
}