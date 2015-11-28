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
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A marker node that takes the result of a child and marks it as 
	/// a name that can later be retrieved.
	/// </summary>
	/// <remarks>
	/// This is useful for implementing things such as outer joins.
	/// </remarks>
	[Serializable]
	class MarkerNode : SingleQueryPlanNode {
		public MarkerNode(IQueryPlanNode child, string markName)
			: base(child) {
			MarkName = markName;
		}

		private MarkerNode(ObjectData data)
			: base(data) {
			MarkName = data.GetString("Marker");
		}

		public override ITable Evaluate(IRequest context) {
			ITable childTable = Child.Evaluate(context);
			context.Query.CacheTable(MarkName, childTable);
			return childTable;
		}

		public string MarkName { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("Marker", MarkName);
		}
	}
}