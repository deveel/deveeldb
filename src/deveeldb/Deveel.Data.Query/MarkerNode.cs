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
	/// A marker node that takes the result of a child and marks it as 
	/// a name that can later be retrieved.
	/// </summary>
	/// <remarks>
	/// This is useful for implementing things such as outer joins.
	/// </remarks>
	[Serializable]
	public class MarkerNode : SingleQueryPlanNode {
		/// <summary>
		/// The name of this mark.
		/// </summary>
		private readonly string markName;

		public MarkerNode(IQueryPlanNode child, string markName)
			: base(child) {
			this.markName = markName;
		}

		public override Table Evaluate(IQueryContext context) {
			Table childTable = Child.Evaluate(context);
			context.AddMarkedTable(markName, childTable);
			return childTable;
		}
	}
}