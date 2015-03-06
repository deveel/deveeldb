// 
//  Copyright 2010-2015 Deveel
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
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	sealed class PlanTableSource {
		public PlanTableSource(QueryPlanNode plan, ObjectName[] variables, string[] uniqueNames) {
			Plan = plan;
			Variables = variables;
			UniqueNames = uniqueNames;
		}

		public QueryPlanNode Plan { get; set; }

		public ObjectName[] Variables { get; set; }

		public string[] UniqueNames { get; set; }

		public JoinType LeftJoinType { get; private set; }

		public PlanTableSource LeftPlan { get; private set; }

		public SqlExpression LeftOnExpression { get; private set; }

		public JoinType RightJoinType { get; private set; }

		public PlanTableSource RightPlan { get; private set; }

		public SqlExpression RightOnExpression { get; private set; }

		public bool IsUpdated { get; set; }

		public bool HasVariable(ObjectName variableName) {
			return Variables.Contains(variableName);
		}

		public void SetLeftJoin(PlanTableSource plan, JoinType joinType, SqlExpression onExpression) {
			LeftPlan = plan;
			LeftJoinType = joinType;
			LeftOnExpression = onExpression;
		}

		public void SetRightJoin(PlanTableSource plan, JoinType joinType, SqlExpression onExpression) {
			RightPlan = plan;
			LeftJoinType = joinType;
			LeftOnExpression = onExpression;
		}

		public void SetJoinMergedBetween(PlanTableSource left, PlanTableSource right) {
			if (left.RightPlan != right) {
				if (left.RightPlan != null) {
					SetRightJoin(left.RightPlan, left.RightJoinType, left.RightOnExpression);
					RightPlan.LeftPlan = this;
				}
				if (right.LeftPlan != null) {
					SetLeftJoin(right.LeftPlan, right.LeftJoinType, right.LeftOnExpression);
					LeftPlan.RightPlan = this;
				}
			}
			if (left.LeftPlan != right) {
				if (LeftPlan == null && left.LeftPlan != null) {
					SetLeftJoin(left.LeftPlan, left.LeftJoinType, left.LeftOnExpression);
					LeftPlan.RightPlan = this;
				}
				if (RightPlan == null && right.RightPlan != null) {
					SetRightJoin(right.RightPlan, right.RightJoinType, right.RightOnExpression);
					RightPlan.LeftPlan = this;
				}
			}
		}

		public void UpdatePlan(QueryPlanNode plan) {
			Plan = plan;
			IsUpdated = true;
		}

		public PlanTableSource Copy() {
			return new PlanTableSource(Plan, Variables, UniqueNames);
		}
	}
}