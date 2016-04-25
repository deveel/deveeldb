// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Routines;
using Deveel.Data.Sql.Objects;

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
			var field = constant.Value;
			if (!Field.IsNullField(field)) {
				var value = field.Value;
				if (value is SqlArray) {
					var array = (SqlArray) value;
					for (int i = 0; i < array.Length; i++) {
						Visit(array.GetValue(i));
					}
				}
			}

			return base.VisitConstant(constant);
		}

		public bool HasAggregate(SqlExpression expression) {
			Visit(expression);
			return aggFunFound;
		}
	}
}
