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
	public sealed class SelectStatement : Statement {
		internal SelectStatement() {
		}

		public SelectStatement(SqlQueryExpression queryExpression) 
			: this(queryExpression, null) {
		}

		public SelectStatement(SqlQueryExpression queryExpression, IEnumerable<ByColumn> orderBy) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			QueryExpression = queryExpression;
		}

		public SqlQueryExpression QueryExpression {
			get { return GetValue<SqlQueryExpression>(Keys.QueryExpression); }
			private set { SetValue(Keys.QueryExpression, value); }
		}

		public IList<ByColumn> OrderBy {
			get { return GetList<ByColumn>(Keys.OrderBy); }
			set { SetValue(Keys.OrderBy, value); }
		}

		protected override PreparedStatement OnPrepare(IQueryContext context) {
			// Prepare this object from the StatementTree,
			// The select expression itself
			var selectExpression = QueryExpression;
			// The order by information
			var orderBy = OrderBy;

			// Form the plan
			var plan = context.DatabaseContext.QueryPlanner().PlanQuery(context.Session, selectExpression);

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
		sealed class PreparedSelectStatement : PreparedStatement {
			public PreparedSelectStatement(IQueryPlanNode queryPlan) {
				if (queryPlan == null)
					throw new ArgumentNullException("queryPlan");

				QueryPlan = queryPlan;
			}

			public IQueryPlanNode QueryPlan { get; private set; }

			protected override ITable OnEvaluate(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
