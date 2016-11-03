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

namespace Deveel.Data.Sql.Expressions.Build {
	class ExpressionBuilder : IExpressionBuilder {
		private SqlExpression expression;
		private SqlExpressionType? prevUnaryType;

		private void VerifyUnary() {
			if (prevUnaryType != null) {
				expression = SqlExpression.Unary(prevUnaryType.Value, expression);
				prevUnaryType = null;
			}
		}

		public IExpressionBuilder Value(object value) {
			expression = SqlExpression.Constant(value);
			VerifyUnary();

			return this;
		}

		public IExpressionBuilder Binary(SqlExpressionType binaryType, Action<IExpressionBuilder> right) {
			if (expression == null)
				throw new InvalidOperationException();

			var builder = new ExpressionBuilder();
			right(builder);

			expression = SqlExpression.Binary(expression, binaryType, builder.Build());

			VerifyUnary();

			return this;
		}

		public IExpressionBuilder Unary(SqlExpressionType unaryType) {
			prevUnaryType = unaryType;
			return this;
		}

		public IExpressionBuilder Query(Action<IQueryExpressionBuilder> query) {
			var builder = new QueryExpressionBuilder();
			query(builder);

			expression = builder.Build();

			return this;
		}

		public IExpressionBuilder Function(ObjectName functionName, params SqlExpression[] args) {
			expression = SqlExpression.FunctionCall(functionName, args);

			VerifyUnary();

			return this;
		}

		public IExpressionBuilder Reference(ObjectName referenceName) {
			expression = SqlExpression.Reference(referenceName);
			VerifyUnary();

			return this;
		}

		public IExpressionBuilder Variable(string variableName) {
			expression = SqlExpression.VariableReference(variableName);

			VerifyUnary();

			return this;
		}

		public IExpressionBuilder Quantified(SqlExpressionType quantifyType, Action<IExpressionBuilder> exp) {
			var builder = new ExpressionBuilder();
			exp(builder);

			expression = SqlExpression.Quantified(quantifyType, builder.Build());

			return this;
		}

		public SqlExpression Build() {
			return expression;
		}
	}
}
