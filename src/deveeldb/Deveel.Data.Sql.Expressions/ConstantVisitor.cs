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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	class ConstantVisitor : SqlExpressionVisitor {
		public ConstantVisitor() {
			IsConstant = true;
		}

		public bool IsConstant { get; private set; }

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (value.Type.TypeCode == SqlTypeCode.Array) {
				var array = value.Value as SqlArray;
				if (array != null && !array.IsNull) {
					foreach (var exp in array) {
						if (!exp.IsConstant()) {
							IsConstant = false;
							break;
						}
					}
				}
			}

			return base.VisitConstant(constant);
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			IsConstant = false;
			return base.VisitReference(reference);
		}

		public override SqlExpression VisitVariableReference(SqlVariableReferenceExpression reference) {
			IsConstant = false;
			return base.VisitVariableReference(reference);
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			return base.VisitFunctionCall(expression);
		}

		public override SqlExpression VisitQuery(SqlQueryExpression query) {
			return base.VisitQuery(query);
		}
	}
}