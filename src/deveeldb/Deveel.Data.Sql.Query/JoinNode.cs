using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class JoinNode : BranchQueryPlanNode {
		public JoinNode(IQueryPlanNode left, IQueryPlanNode right, ObjectName leftColumnName, SqlExpressionType @operator, SqlExpression rightExpression) 
			: base(left, right) {
			LeftColumnName = leftColumnName;
			Operator = @operator;
			RightExpression = rightExpression;
		}

		public ObjectName LeftColumnName { get; private set; }

		public SqlExpressionType Operator { get; private set; }

		public SqlExpression RightExpression { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			throw new NotImplementedException();
		}
	}
}
