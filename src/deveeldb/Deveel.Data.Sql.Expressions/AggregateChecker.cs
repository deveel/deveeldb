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

using Deveel.Data;
using Deveel.Data.Routines;

namespace Deveel.Data.Sql.Expressions {
	class AggregateChecker : SqlExpressionVisitor {
		private readonly IRequest query;
		private bool aggFunFound;

		public AggregateChecker(IRequest query) {
			this.query = query;
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			var invoke = new Invoke(expression.FunctioName, expression.Arguments);
			aggFunFound = invoke.IsAggregate(query);

			return base.VisitFunctionCall(expression);
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			// TODO: if the value is an array iterate
			return base.VisitConstant(constant);
		}

		public bool HasAggregate(SqlExpression expression) {
			Visit(expression);
			return aggFunFound;
		}
	}
}
