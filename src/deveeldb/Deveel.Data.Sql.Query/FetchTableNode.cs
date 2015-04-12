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
	class FetchTableNode : IQueryPlanNode {
		public FetchTableNode(ObjectName tableName, ObjectName aliasName) {
			this.TableName = tableName;
			this.AliasName = aliasName;
		}

		/// <summary>
		/// The name of the table to fetch.
		/// </summary>
		public ObjectName TableName { get; private set; }

		/// <summary>
		/// The name to alias the table as.
		/// </summary>
		public ObjectName AliasName { get; private set; }


		public ITable Evaluate(IQueryContext context) {
			var t = context.GetTable(TableName);
			return AliasName != null ? new ReferenceTable(t, AliasName) : t;
		}
	}
}