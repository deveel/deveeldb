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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	internal class ConstantExpressionPlan : ExpressionPlan {
		private readonly SqlExpression expression;

		public ConstantExpressionPlan(QueryTableSetPlanner planner, SqlExpression expression)
			: base(planner) {
			this.expression = expression;
		}

		public override void AddToPlanTree() {
			// Each currently open branch must have this constant expression added
			// to it.
			foreach (var plan in TableSetPlanner.Sources)
				plan.UpdatePlan(new ConstantSelectNode(plan.Plan, expression));
		}
	}
}