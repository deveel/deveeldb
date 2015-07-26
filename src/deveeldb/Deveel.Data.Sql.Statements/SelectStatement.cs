using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	public sealed class SelectStatement : SqlStatement {
		public SelectStatement(SqlQueryExpression queryExpression) 
			: this(queryExpression, null) {
		}

		public SelectStatement(SqlQueryExpression queryExpression, IEnumerable<SortColumn> orderBy) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			QueryExpression = queryExpression;
			OrderBy = orderBy;
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public IEnumerable<SortColumn> OrderBy { get; set; }

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var queryPlan = context.DatabaseContext().QueryPlanner().PlanQuery(context, QueryExpression, OrderBy);
			return new PreparedSelectStatement(queryPlan);
		}

		#region PreparedSelectStatement

		class PreparedSelectStatement : SqlPreparedStatement {
			public PreparedSelectStatement(IQueryPlanNode queryPlan) {
				QueryPlan = queryPlan;
			}

			public IQueryPlanNode QueryPlan { get; private set; }

			public override ITable Evaluate(IQueryContext context) {
				return QueryPlan.Evaluate(context);
			}
		}

		#endregion
	}
}
