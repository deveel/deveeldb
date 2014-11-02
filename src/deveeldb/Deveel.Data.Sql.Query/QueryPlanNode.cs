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
	abstract class QueryPlanNode : IQueryPlanNode {
		public abstract ITable Evaluate(IQueryContext context);

		protected virtual void OnAcceptVisit(IQueryPlanNodeVisitor visitor) {
		}

		public abstract IList<QueryReference> DiscoverQueryReferences(int queryLevel, IList<QueryReference> list);

		public abstract IList<ObjectName> DiscoverTableNames(IList<ObjectName> list);

		void IQueryPlanNode.Accept(IQueryPlanNodeVisitor visitor) {
			OnAcceptVisit(visitor);
		}
	}
}