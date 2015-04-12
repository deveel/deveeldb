using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class LogicalUnionNode : BranchQueryPlanNode {
		public LogicalUnionNode(IQueryPlanNode left, IQueryPlanNode right) 
			: base(left, right) {
		}

		public override ITable Evaluate(IQueryContext context) {
			var leftResult = Left.Evaluate(context);
			var rightResult = Right.Evaluate(context);

			return leftResult.Union(rightResult);
		}
	}
}
