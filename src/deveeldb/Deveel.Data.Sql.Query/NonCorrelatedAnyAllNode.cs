using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class NonCorrelatedAnyAllNode : BranchQueryPlanNode {
		public NonCorrelatedAnyAllNode(IQueryPlanNode left, IQueryPlanNode right, ObjectName[] leftColumnNames, SqlExpressionType subQueryType) 
			: base(left, right) {
			LeftColumnNames = leftColumnNames;
			SubQueryType = subQueryType;
		}

		public ObjectName[] LeftColumnNames { get; private set; }

		public SqlExpressionType SubQueryType { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			// Solve the left branch result
			var leftResult = Left.Evaluate(context);
			// Solve the right branch result
			var rightResult = Right.Evaluate(context);

			// Solve the sub query on the left columns with the right plan and the
			// given operator.
			return leftResult.AnyAllNonCorrelated(LeftColumnNames, SubQueryType, rightResult);
		}
	}
}
