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
using Deveel.Data.Routines;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	class ReturnTypeVisitor : SqlExpressionVisitor {
		private readonly IQueryContext queryContext;
		private readonly IVariableResolver variableResolver;

		private DataType dataType;

		public ReturnTypeVisitor(IQueryContext queryContext, IVariableResolver variableResolver) {
			this.queryContext = queryContext;
			this.variableResolver = variableResolver;

			dataType = PrimitiveTypes.Null();
		}

		public override SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
			dataType = binaryEpression.BinaryOperator.ResultType;
			return base.VisitBinary(binaryEpression);
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			dataType = constant.Value.Type;

			return base.VisitConstant(constant);
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			var invoke = new InvokeRequest(expression.FunctioName, expression.Arguments);
			var function = invoke.ResolveRoutine(queryContext) as IFunction;
			if (function != null)
				dataType = function.ReturnType(invoke, queryContext, variableResolver);

			return base.VisitFunctionCall(expression);
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			var name = reference.ReferenceName;
			if (reference.IsToVariable) {
				// TODO: resolve the global variable and get the type...
			} else {
				dataType = variableResolver.ReturnType(name);
			}

			return base.VisitReference(reference);
		}

		public override SqlExpression Visit(SqlExpression expression) {
			if (expression is QueryReferenceExpression)
				return VisitQueryReference((QueryReferenceExpression) expression);

			return base.Visit(expression);
		}

		private SqlExpression VisitQueryReference(QueryReferenceExpression expression) {
			dataType = expression.Reference.ReturnType;
			return expression;
		}

		public DataType GetType(SqlExpression expression) {
			Visit(expression);
			return dataType;
		}
	}
}
