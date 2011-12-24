using System;
using System.Collections.Generic;

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// A branch node for a non-equi join between two tables.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> The cost of a LeftJoin is higher if the right child 
	/// result is greater than the left child result. The plan should be 
	/// arranged so smaller results are on the left.
	/// </remarks>
	[Serializable]
	public class JoinNode : BranchQueryPlanNode {
		/// <summary>
		/// The variable in the left table to be joined.
		/// </summary>
		private VariableName leftVar;

		/// <summary>
		/// The operator to join under (=, &lt;&gt;, &gt;, &lt;, &gt;=, &lt;=).
		/// </summary>
		private readonly Operator joinOp;

		/// <summary>
		/// The expression evaluated on the right table.
		/// </summary>
		private Expression rightExpression;

		public JoinNode(IQueryPlanNode left, IQueryPlanNode right, VariableName leftVar, Operator joinOp, Expression rightExpression)
			: base(left, right) {
			this.leftVar = leftVar;
			this.joinOp = joinOp;
			this.rightExpression = rightExpression;
		}

		public override Table Evaluate(IQueryContext context) {
			// Solve the left branch result
			Table leftResult = Left.Evaluate(context);
			// Solve the right branch result
			Table rightResult = Right.Evaluate(context);

			// If the rightExpression is a simple variable then we have the option
			// of optimizing this join by putting the smallest table on the LHS.
			VariableName rhsVar = rightExpression.AsVariableName();
			VariableName lhsVar = leftVar;
			Operator op = joinOp;
			if (rhsVar != null) {
				// We should arrange the expression so the right table is the smallest
				// of the sides.
				// If the left result is less than the right result
				if (leftResult.RowCount < rightResult.RowCount) {
					// Reverse the join
					rightExpression = new Expression(lhsVar);
					lhsVar = rhsVar;
					op = op.Reverse();
					// Reverse the tables.
					Table t = rightResult;
					rightResult = leftResult;
					leftResult = t;
				}
			}

			// The join operation.
			return leftResult.SimpleJoin(context, rightResult, lhsVar, op, rightExpression);
		}

		public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return rightExpression.DiscoverTableNames(base.DiscoverTableNames(list));
		}

		public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return rightExpression.DiscoverCorrelatedVariables(ref level, base.DiscoverCorrelatedVariables(level, list));
		}

		public override Object Clone() {
			JoinNode node = (JoinNode)base.Clone();
			node.leftVar = (VariableName)leftVar.Clone();
			node.rightExpression = (Expression)rightExpression.Clone();
			return node;
		}

		public override string Title {
			get { return "JOIN: " + leftVar + joinOp + rightExpression; }
		}
	}
}