using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryPlanner : IQueryPlanner {
		public IQueryPlanNode PlanQuery(IUserSession session, SqlQueryExpression queryExpression) {
			throw new NotImplementedException();
		}
	}
}
