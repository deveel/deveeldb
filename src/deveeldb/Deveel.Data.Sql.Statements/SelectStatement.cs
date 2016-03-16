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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SelectStatement : SqlStatement, IPlSqlStatement {
		public SelectStatement(SqlQueryExpression queryExpression) 
			: this(queryExpression, (QueryLimit) null) {
		}

		public SelectStatement(SqlQueryExpression queryExpression, IEnumerable<SortColumn> orderBy) 
			: this(queryExpression, null, orderBy) {
		}

		public SelectStatement(SqlQueryExpression queryExpression, QueryLimit limit) 
			: this(queryExpression, limit, null) {
		}

		public SelectStatement(SqlQueryExpression queryExpression, QueryLimit limit, IEnumerable<SortColumn> orderBy) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			Limit = limit;
			QueryExpression = queryExpression;
			OrderBy = orderBy;
		}

		private SelectStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			QueryExpression = (SqlQueryExpression) info.GetValue("Query", typeof (SqlQueryExpression));
			Limit = (QueryLimit) info.GetValue("Limit", typeof (QueryLimit));

			var orderBy = (SortColumn[]) info.GetValue("OrderBy", typeof (SortColumn[]));
			if (orderBy != null)
				OrderBy = new List<SortColumn>(orderBy);
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public IEnumerable<SortColumn> OrderBy { get; set; }

		public QueryLimit Limit { get; set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Query", QueryExpression);
			info.AddValue("Limit", Limit);

			if (OrderBy != null) {
				var orderBy = OrderBy.ToArray();
				info.AddValue("OrderBy", orderBy);
			} else {
				info.AddValue("OrderBy", null);
			}
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(new QueryInfo(context, QueryExpression, OrderBy, Limit));
			return new Prepared(queryPlan);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(IQueryPlanNode queryPlan) {
				QueryPlan = queryPlan;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				QueryPlan = (IQueryPlanNode) info.GetValue("QueryPlan", typeof(IQueryPlanNode));
			}

			public IQueryPlanNode QueryPlan { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("QueryPlan", QueryPlan);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				var result = QueryPlan.Evaluate(context.Request);
				context.SetResult(result);
			}
		}

		#endregion
	}
}
