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

using Deveel.Data;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	class ReturnTypeVisitor : SqlExpressionVisitor {
		private readonly IRequest query;
		private readonly IVariableResolver variableResolver;

		private SqlType sqlType;

		public ReturnTypeVisitor(IRequest query, IVariableResolver variableResolver) {
			this.query = query;
			this.variableResolver = variableResolver;
		}

		private void SetType(SqlType type) {
			if (sqlType == null)
				sqlType = type;
		}

		public override SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
			switch (binaryEpression.ExpressionType) {
				case SqlExpressionType.Add:
				case SqlExpressionType.Subtract:
				case SqlExpressionType.Multiply:
				case SqlExpressionType.Modulo:
				case SqlExpressionType.Divide:
					SetType(PrimitiveTypes.Numeric());
					break;
				default:
					// we assume the expression type is already been check to be binary.
					SetType(PrimitiveTypes.Boolean());
					break;

			}
			
			return base.VisitBinary(binaryEpression);
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			SetType(constant.Value.Type);

			return base.VisitConstant(constant);
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			var invoke = new Invoke(expression.FunctioName, expression.Arguments);
			var function = invoke.ResolveRoutine(query) as IFunction;
			if (function != null)
				SetType(function.ReturnType(invoke, query, variableResolver));

			return base.VisitFunctionCall(expression);
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			var name = reference.ReferenceName;
			SetType(variableResolver.ReturnType(name));

			return base.VisitReference(reference);
		}

		public override SqlExpression Visit(SqlExpression expression) {
			if (expression is QueryReferenceExpression)
				return VisitQueryReference((QueryReferenceExpression) expression);

			return base.Visit(expression);
		}

		private SqlExpression VisitQueryReference(QueryReferenceExpression expression) {
			SetType(expression.QueryReference.ReturnType);
			return expression;
		}

		public SqlType GetType(SqlExpression expression) {
			Visit(expression);
			return sqlType;
		}
	}
}
