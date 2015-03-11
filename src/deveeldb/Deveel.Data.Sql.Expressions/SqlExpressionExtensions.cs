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

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	public static class SqlExpressionExtensions {
		public static ObjectName AsReferenceName(this SqlExpression expression) {
			var refExpression = expression as SqlReferenceExpression;
			if (refExpression == null)
				return null;

			return refExpression.ReferenceName;
		}

		public static DataObject EvaluateToConstant(this SqlExpression expression, EvaluateContext context) {
			var evalExp = expression.Evaluate(context);
			if (evalExp == null)
				throw new InvalidOperationException();

			var constantExp = evalExp as SqlConstantExpression;
			if (constantExp == null)
				throw new InvalidOperationException();

			return constantExp.Value;
		}

		public static DataObject EvaluateToConstant(this SqlExpression expression, IQueryContext queryContext, IVariableResolver variableResolver) {
			return expression.EvaluateToConstant(new EvaluateContext(queryContext, variableResolver));
		}

		public static DataType ReturnType(this SqlExpression expression, IQueryContext queryContext, IVariableResolver variableResolver) {
			var visitor = new ReturnTypeVisitor(queryContext, variableResolver);
			return visitor.GetType(expression);
		}
	}
}
