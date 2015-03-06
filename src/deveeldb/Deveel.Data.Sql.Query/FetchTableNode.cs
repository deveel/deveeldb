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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// The node for fetching a table from the current transaction.
	/// </summary>
	/// <remarks>
	/// This is a tree node and has no children.
	/// </remarks>
	[Serializable]
	public sealed class FetchTableNode : QueryPlanNode {
		/// <summary>
		/// The name of the table to fetch.
		/// </summary>
		private readonly ObjectName tableName;

		/// <summary>
		/// The name to alias the table as.
		/// </summary>
		private readonly ObjectName aliasName;

		public FetchTableNode(ObjectName tableName, ObjectName aliasName) {
			this.tableName = tableName;
			this.aliasName = aliasName;
		}

		internal override IList<ObjectName> DiscoverTableNames(IList<ObjectName> list) {
			if (!list.Contains(tableName))
				list.Add(tableName);

			return list;
		}

		/// <inheritdoc/>
		public override ITable Evaluate(IQueryContext context) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		internal override IList<QueryReference> DiscoverQueryReferences(int level, IList<QueryReference> list) {
			return list;
		}
	}
}