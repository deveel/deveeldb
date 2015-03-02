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
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SelectStatement : Statement {
		public SqlQueryExpression QueryExpression {
			get { return StatementTree.GetValue<SqlQueryExpression>("QueryExpression"); }
			set { StatementTree.SetValue("QueryExpression", value); }
		}

		public IList<ByColumn> OrderBy {
			get { return StatementTree.GetList<ByColumn>("OrderBy"); }
			set { StatementTree.SetValue("OrderBy", value); }
		}

		protected override PreparedStatement OnPrepare(IQueryContext context) {
			// Prepare this object from the StatementTree,
			// The select expression itself
			var selectExpression = QueryExpression;
			// The order by information
			var orderBy = OrderBy;

			// Form the plan
			var plan = context.SystemContext.QueryPlanner.PlanQuery(context.QueryPlanContext, selectExpression);

			return new PreparedSelectStatement(plan);
		}
	}
}
