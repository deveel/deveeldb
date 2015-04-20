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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SqlSelectStatement : SqlStatement {
		public SqlSelectStatement(SqlQueryExpression queryExpression) 
			: this(queryExpression, null) {
		}

		public SqlSelectStatement(SqlQueryExpression queryExpression, IEnumerable<SortColumn> orderBy) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			QueryExpression = queryExpression;
			OrderBy = new List<SortColumn>();
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public IList<SortColumn> OrderBy { get; private set; }

		public override StatementType StatementType {
			get { return StatementType.Select; }
		}

		protected override SqlPreparedStatement PrepareStatement(IQueryContext context) {
			// Prepare this object from the StatementTree,
			// The select expression itself
			var selectExpression = QueryExpression;
			// The order by information
			var orderBy = OrderBy;

			// Form the plan
			var plan = context.DatabaseContext.QueryPlanner().PlanQuery(context, selectExpression, orderBy);

			return new PreparedSelectStatement(plan);
		}

		#region Keys

		internal static class Keys {
			public const string QueryExpression = "QueryExpression";
			public const string OrderBy = "OrderBy";
		}

		#endregion

		#region PreparedSelectStatement

		[Serializable]
		sealed class PreparedSelectStatement : SqlPreparedStatement {
			public PreparedSelectStatement(IQueryPlanNode queryPlan) {
				if (queryPlan == null)
					throw new ArgumentNullException("queryPlan");

				QueryPlan = queryPlan;
			}

			public IQueryPlanNode QueryPlan { get; private set; }

			public override ITable Evaluate(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
