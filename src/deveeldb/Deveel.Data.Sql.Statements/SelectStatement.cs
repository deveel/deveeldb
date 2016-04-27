// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Transactions;

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

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var preparedQuery = QueryExpression.Prepare(preparer);
			if (!(preparedQuery is SqlQueryExpression))
				throw new StatementException("The preparation of the query expression resulted in an invalid expression.");

			var orderBy = new List<SortColumn>();
			if (OrderBy != null) {
				foreach (var column in OrderBy) {
					var prepared = (SortColumn) ((IPreparable) column).Prepare(preparer);
					orderBy.Add(prepared);
				}
			}

			var query = (SqlQueryExpression) preparedQuery;

			return new SelectStatement(query, Limit, orderBy);
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
				// TODO: Verify if a native cursor is already opened..
				
				if (!context.User.CanSelectFrom(QueryPlan))
					throw new SecurityException(String.Format("The user '{0}' has not enough rights to select from the query.", context.User.Name));

				var cursorInfo = new NativeCursorInfo(QueryPlan);
				var nativeCursor = new NativeCursor(cursorInfo, context.Request);

				context.SetCursor(nativeCursor);
			}
		}

		#endregion
	}
}
