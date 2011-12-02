using System;

namespace Deveel.Data.Sql {
	internal sealed partial class Planner {
		/// <summary>
		/// Represents a single table source being planned.
		/// </summary>
		private sealed class PlanTableSource {
			/// <summary>
			/// The Plan for this table source.
			/// </summary>
			private IQueryPlanNode plan;

			/// <summary>
			/// The list of fully qualified Variable objects that are accessable 
			/// within this plan.
			/// </summary>
			private readonly VariableName[] varList;

			/// <summary>
			/// The list of unique key names of the tables in this plan.
			/// </summary>
			private readonly string[] uniqueNames;

			/// <summary>
			/// Set to true when this source has been updated from when it was
			/// constructed or copied.
			/// </summary>
			private bool isUpdated;

			// How this plan is naturally joined to other plans in the source.  A
			// plan either has no dependance, a left or a right dependance, or a left
			// and right dependance.
			private PlanTableSource leftPlan;
			private PlanTableSource rightPlan;
			private JoinType leftJoinType;
			private JoinType rightJoinType;
			private Expression leftOnExpr;
			private Expression rightOnExpr;


			public PlanTableSource(IQueryPlanNode plan, VariableName[] varList, String[] uniqueNames) {
				this.plan = plan;
				this.varList = varList;
				this.uniqueNames = uniqueNames;
				leftJoinType = JoinType.None;
				rightJoinType = JoinType.None;
				isUpdated = false;
			}

			/// <summary>
			/// Sets the left join information for this plan.
			/// </summary>
			/// <param name="left_plan"></param>
			/// <param name="join_type"></param>
			/// <param name="on_expr"></param>
			public void SetLeftJoinInfo(PlanTableSource left_plan, JoinType join_type, Expression on_expr) {
				this.leftPlan = left_plan;
				this.leftJoinType = join_type;
				this.leftOnExpr = on_expr;
			}

			/// <summary>
			/// Sets the right join information for this plan.
			/// </summary>
			/// <param name="right_plan"></param>
			/// <param name="join_type"></param>
			/// <param name="on_expr"></param>
			public void SetRightJoinInfo(PlanTableSource right_plan, JoinType join_type, Expression on_expr) {
				this.rightPlan = right_plan;
				this.rightJoinType = join_type;
				this.rightOnExpr = on_expr;
			}

			/// <summary>
			/// This is called when two plans are merged together to set 
			/// up the left and right join information for the new plan.
			/// </summary>
			/// <param name="left"></param>
			/// <param name="right"></param>
			/// <remarks>
			/// This sets the left join info from the left plan and the 
			/// right join info from the right plan.
			/// </remarks>
			public void SetJoinInfoMergedBetween(PlanTableSource left, PlanTableSource right) {
				if (left.rightPlan != right) {
					if (left.rightPlan != null) {
						SetRightJoinInfo(left.rightPlan, left.rightJoinType, left.rightOnExpr);
						rightPlan.leftPlan = this;
					}
					if (right.leftPlan != null) {
						SetLeftJoinInfo(right.leftPlan, right.leftJoinType, right.leftOnExpr);
						leftPlan.rightPlan = this;
					}
				}
				if (left.leftPlan != right) {
					if (leftPlan == null && left.leftPlan != null) {
						SetLeftJoinInfo(left.leftPlan, left.leftJoinType, left.leftOnExpr);
						leftPlan.rightPlan = this;
					}
					if (rightPlan == null && right.rightPlan != null) {
						SetRightJoinInfo(right.rightPlan, right.rightJoinType, right.rightOnExpr);
						rightPlan.leftPlan = this;
					}
				}

			}

			/// <summary>
			/// Returns true if this table source contains the variable reference.
			/// </summary>
			/// <param name="v"></param>
			/// <returns></returns>
			public bool ContainsVariable(VariableName v) {
				//      Console.Out.WriteLine("Looking for: " + v);
				for (int i = 0; i < varList.Length; ++i) {
					//        Console.Out.WriteLine(varList[i]);
					if (varList[i].Equals(v)) {
						return true;
					}
				}
				return false;
			}

			/// <summary>
			/// Checks if the plan contains the given unique table name.
			/// </summary>
			/// <param name="name"></param>
			/// <returns>
			/// Returns <b>true</b> if this table source contains the 
			/// unique table name reference, otherwise <b>false</b>.
			/// </returns>
			public bool ContainsUniqueKey(String name) {
				for (int i = 0; i < uniqueNames.Length; ++i) {
					if (uniqueNames[i].Equals(name)) {
						return true;
					}
				}
				return false;
			}

			/// <summary>
			/// Sets the updated flag.
			/// </summary>
			public void SetUpdated() {
				isUpdated = true;
			}

			/// <summary>
			/// Updates the plan.
			/// </summary>
			/// <param name="node"></param>
			public void UpdatePlan(IQueryPlanNode node) {
				plan = node;
				SetUpdated();
			}

			/// <summary>
			/// Returns the plan for this table source.
			/// </summary>
			public IQueryPlanNode Plan {
				get { return plan; }
				set { plan = value; }
			}

			/// <summary>
			/// Returns true if the planner was updated.
			/// </summary>
			public bool IsUpdated {
				get { return isUpdated; }
			}

			public PlanTableSource LeftPlan {
				get { return leftPlan; }
				set { leftPlan = value; }
			}

			public PlanTableSource RightPlan {
				get { return rightPlan; }
				set { rightPlan = value; }
			}

			public JoinType LeftJoinType {
				get { return leftJoinType; }
				set { leftJoinType = value; }
			}

			public JoinType RightJoinType {
				get { return rightJoinType; }
				set { rightJoinType = value; }
			}

			public Expression LeftOnExpression {
				get { return leftOnExpr; }
				set { leftOnExpr = value; }
			}

			public Expression RightOnExpression {
				get { return rightOnExpr; }
				set { rightOnExpr = value; }
			}

			/// <summary>
			/// The list of fully qualified Variable objects that are accessable 
			/// within this plan.
			/// </summary>
			public VariableName[] Variables {
				get { return varList; }
			}

			/// <summary>
			/// The list of unique key names of the tables in this plan.
			/// </summary>
			public string[] UniqueNames {
				get { return uniqueNames; }
			}

			/// <summary>
			/// Makes a copy of this table source.
			/// </summary>
			/// <returns></returns>
			public PlanTableSource Copy() {
				return new PlanTableSource(plan, varList, uniqueNames);
			}
		} 
	}
}