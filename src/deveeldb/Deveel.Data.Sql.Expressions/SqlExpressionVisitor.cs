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

namespace Deveel.Data.Sql.Expressions {
	public class SqlExpressionVisitor {
		public SqlExpression Visit(SqlExpression expression) {
			if (expression == null)
				return null;

			var expressionType = expression.ExpressionType;
			switch (expressionType) {
				case SqlExpressionType.Add:
				case SqlExpressionType.Subtract:
				case SqlExpressionType.Divide:
				case SqlExpressionType.Multiply:
				case SqlExpressionType.Modulo:
				case SqlExpressionType.And:
				case SqlExpressionType.Or:
				case SqlExpressionType.XOr:
				case SqlExpressionType.Equal:
				case SqlExpressionType.NotEqual:
				case SqlExpressionType.Like:
				case SqlExpressionType.NotLike:
				case SqlExpressionType.GreaterThan:
				case SqlExpressionType.GreaterOrEqualThan:
				case SqlExpressionType.SmallerThan:
				case SqlExpressionType.SmallerOrEqualThan:
				case SqlExpressionType.Is:
				case SqlExpressionType.IsNot:
					return VisitBinary((SqlBinaryExpression) expression);
				case SqlExpressionType.Negate:
				case SqlExpressionType.Not:
				case SqlExpressionType.UnaryPlus:
					return VisitUnary((SqlUnaryExpression) expression);
				case SqlExpressionType.Cast:
					return VisitCast((SqlCastExpression) expression);
				case SqlExpressionType.Reference:
					return VisitReference((SqlReferenceExpression) expression);
				case SqlExpressionType.Assign:
					return VisitAssign((SqlAssignExpression) expression);
				case SqlExpressionType.FunctionCall:
					return VisitFunctionCall((SqlFunctionCallExpression) expression);
				case SqlExpressionType.Constant:
					return VisitConstant((SqlConstantExpression) expression);
				case SqlExpressionType.Conditional:
					return VisitConditional((SqlConditionalExpression) expression);
				case SqlExpressionType.Query:
					return VisitQuery((SqlQueryExpression) expression);
				case SqlExpressionType.Tuple:
					return VisitTuple((SqlTupleExpression) expression);
				default:
					return expression.Accept(this);
			}
		}

		public virtual SqlExpression[] VisitExpressionList(SqlExpression[] list) {
			if (list == null)
				return new SqlExpression[0];

			var result = new SqlExpression[list.Length];
			for (int i = 0; i < list.Length; i++) {
				result[i] = Visit(list[i]);
			}

			return result;
		}

		public virtual SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			var ags = VisitExpressionList(expression.Arguments);
			return SqlExpression.FunctionCall(expression.FunctioName, ags);
		}

		public virtual SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
			var left = binaryEpression.Left;
			var right = binaryEpression.Right;
			if (left != null)
				left = Visit(left);
			if (right != null)
				right = Visit(right);

			return SqlExpression.Binary(left, binaryEpression.ExpressionType, right);
		}

		public virtual SqlExpression VisitUnary(SqlUnaryExpression unary) {
			var operand = unary.Operand;
			if (operand != null)
				operand = Visit(operand);

			return SqlExpression.Unary(unary.ExpressionType, operand);
		}

		public virtual SqlExpression VisitCast(SqlCastExpression castExpression) {
			// TODO:
			return castExpression;
		}

		public virtual SqlExpression VisitReference(SqlReferenceExpression reference) {
			if (reference.IsToVariable)
				return SqlExpression.VariableReference(reference.ReferenceName.FullName);
			return SqlExpression.Reference(reference.ReferenceName);
		}

		public virtual SqlExpression VisitAssign(SqlAssignExpression assign) {
			var reference = assign.Reference;
			var expression = assign.Expression;
			if (reference != null)
				reference = Visit(reference);
			if (expression != null)
				expression = Visit(expression);

			return SqlExpression.Assign(reference, expression);
		}

		public virtual SqlExpression VisitConstant(SqlConstantExpression constant) {
			return SqlExpression.Constant(constant.Value);
		}

		public virtual SqlExpression VisitConditional(SqlConditionalExpression conditional) {
			var test = conditional.TestExpression;
			var ifTrue = conditional.TrueExpression;
			var ifFalse = conditional.FalseExpression;

			if (test != null)
				test = Visit(test);
			if (ifTrue != null)
				ifTrue = Visit(ifTrue);
			if (ifFalse != null)
				ifFalse = Visit(ifFalse);

			return SqlExpression.Conditional(test, ifTrue, ifFalse);
		}

		public virtual SqlExpression VisitTuple(SqlTupleExpression expression) {
			var list = VisitExpressionList(expression.Expressions);
			return SqlExpression.Tuple(list);
		}

		public virtual SqlExpression VisitQuery(SqlQueryExpression query) {
			// TODO: This is too complex to visit now ... let's do it later
			return query;
		}
	}
}