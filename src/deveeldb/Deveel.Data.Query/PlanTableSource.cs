// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Query {
	/// <summary>
	/// Represents a single table source being planned.
	/// </summary>
	internal sealed class PlanTableSource {
		// How this plan is naturally joined to other plans in the source.  A
		// plan either has no dependance, a left or a right dependance, or a left
		// and right dependance.

		public PlanTableSource(IQueryPlanNode plan, VariableName[] variables, string[] uniqueNames) {
			Plan = plan;
			VariableNames = variables;
			UniqueNames = uniqueNames;
			LeftJoinType = JoinType.None;
			RightJoinType = JoinType.None;
			IsUpdated = false;
		}

		/// <summary>
		/// Returns the plan for this table source.
		/// </summary>
		public IQueryPlanNode Plan { get; set; }

		/// <summary>
		/// Returns true if the planner was updated.
		/// </summary>
		public bool IsUpdated { get; private set; }

		/// <summary>
		/// The list of fully qualified Variable objects that are accessable 
		/// within this plan.
		/// </summary>
		public VariableName[] VariableNames { get; private set; }

		/// <summary>
		/// The list of unique key names of the tables in this plan.
		/// </summary>
		public string[] UniqueNames { get; private set; }

		public PlanTableSource LeftPlan { get; set; }

		public PlanTableSource RightPlan { get; set; }

		public JoinType LeftJoinType { get; set; }

		public JoinType RightJoinType { get; set; }

		public Expression LeftOnExpression { get; set; }

		public Expression RightOnExpression { get; set; }

		/// <summary>
		/// Sets the left join information for this plan.
		/// </summary>
		/// <param name="left"></param>
		/// <param name="joinType"></param>
		/// <param name="onExpression"></param>
		public void SetLeftJoinInfo(PlanTableSource left, JoinType joinType, Expression onExpression) {
			LeftPlan = left;
			LeftJoinType = joinType;
			LeftOnExpression = onExpression;
		}

		/// <summary>
		/// Sets the right join information for this plan.
		/// </summary>
		/// <param name="right"></param>
		/// <param name="joinType"></param>
		/// <param name="onExpression"></param>
		public void SetRightJoinInfo(PlanTableSource right, JoinType joinType, Expression onExpression) {
			RightPlan = right;
			RightJoinType = joinType;
			RightOnExpression = onExpression;
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
			if (left.RightPlan != right) {
				if (left.RightPlan != null) {
					SetRightJoinInfo(left.RightPlan, left.RightJoinType, left.RightOnExpression);
					RightPlan.LeftPlan = this;
				}
				if (right.LeftPlan != null) {
					SetLeftJoinInfo(right.LeftPlan, right.LeftJoinType, right.LeftOnExpression);
					LeftPlan.RightPlan = this;
				}
			}
			if (left.LeftPlan != right) {
				if (LeftPlan == null && left.LeftPlan != null) {
					SetLeftJoinInfo(left.LeftPlan, left.LeftJoinType, left.LeftOnExpression);
					LeftPlan.RightPlan = this;
				}
				if (RightPlan == null && right.RightPlan != null) {
					SetRightJoinInfo(right.RightPlan, right.RightJoinType, right.RightOnExpression);
					RightPlan.LeftPlan = this;
				}
			}

		}

		/// <summary>
		/// Returns true if this table source contains the variable reference.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		public bool ContainsVariable(VariableName v) {
			return VariableNames.Contains(v);
		}

		/// <summary>
		/// Checks if the plan contains the given unique table name.
		/// </summary>
		/// <param name="name"></param>
		/// <returns>
		/// Returns <b>true</b> if this table source contains the 
		/// unique table name reference, otherwise <b>false</b>.
		/// </returns>
		public bool ContainsUniqueKey(string name) {
			return UniqueNames.Any(uniqueName => uniqueName.Equals(name));
		}

		/// <summary>
		/// Sets the updated flag.
		/// </summary>
		public void SetUpdated() {
			IsUpdated = true;
		}

		/// <summary>
		/// Updates the plan.
		/// </summary>
		/// <param name="node"></param>
		public void UpdatePlan(IQueryPlanNode node) {
			Plan = node;
			SetUpdated();
		}

		/// <summary>
		/// Makes a copy of this table source.
		/// </summary>
		/// <returns></returns>
		public PlanTableSource Copy() {
			return new PlanTableSource(Plan, VariableNames, UniqueNames);
		}
	}
}