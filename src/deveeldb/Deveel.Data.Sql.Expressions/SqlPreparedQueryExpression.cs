using System;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlPreparedQueryExpression : SqlExpression {
		internal SqlPreparedQueryExpression(IQueryPlanNode queryPlan) {
			if (queryPlan == null)
				throw new ArgumentNullException("queryPlan");

			QueryPlan = queryPlan;
		}

		public IQueryPlanNode QueryPlan { get; private set; }

		public override bool CanEvaluate {
			get { return false; }
		}

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.PreparedQuery; }
		}
	}
}