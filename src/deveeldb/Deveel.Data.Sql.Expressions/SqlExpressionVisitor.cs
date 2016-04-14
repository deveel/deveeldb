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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// A visitor for <see cref="SqlExpression"/> objects.
	/// </summary>
	public class SqlExpressionVisitor {
		/// <summary>
		/// Visits a given SQL expression.
		/// </summary>
		/// <param name="expression">The <see cref="SqlExpression"/> to visit.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> as result of the visit.
		/// </returns>
		public virtual SqlExpression Visit(SqlExpression expression) {
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
				case SqlExpressionType.VariableReference:
					return VisitVariableReference((SqlVariableReferenceExpression) expression);
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
				case SqlExpressionType.All:
				case SqlExpressionType.Any:
					return VisitQuantified((SqlQuantifiedExpression) expression);
				default:
					return expression.Accept(this);
			}
		}

		/// <summary>
		/// Visits a list of expressions given.
		/// </summary>
		/// <param name="list">The list of <see cref="SqlExpression"/> to visit.</param>
		/// <remarks>
		/// The default implementation iterates the given list and visits
		/// any expression contained.
		/// </remarks>
		/// <returns>
		/// Returns a new array of <see cref="SqlExpression"/> resulted from the
		/// visit to each of the input expressions.
		/// </returns>
		public virtual SqlExpression[] VisitExpressionList(SqlExpression[] list) {
			if (list == null)
				return new SqlExpression[0];

			var result = new SqlExpression[list.Length];
			for (int i = 0; i < list.Length; i++) {
				result[i] = Visit(list[i]);
			}

			return result;
		}

		/// <summary>
		/// Visits the expression that calls the function defined.
		/// </summary>
		/// <param name="expression">The <see cref="SqlFunctionCallExpression"/> to visit.</param>
		/// <returns></returns>
		public virtual SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			var ags = VisitExpressionList(expression.Arguments);
			return SqlExpression.FunctionCall(expression.FunctioName, ags);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binaryEpression"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
			var left = binaryEpression.Left;
			var right = binaryEpression.Right;
			if (left != null)
				left = Visit(left);
			if (right != null)
				right = Visit(right);

			return SqlExpression.Binary(left, binaryEpression.ExpressionType, right);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="unary"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitUnary(SqlUnaryExpression unary) {
			var operand = unary.Operand;
			if (operand != null)
				operand = Visit(operand);

			return SqlExpression.Unary(unary.ExpressionType, operand);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="castExpression"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitCast(SqlCastExpression castExpression) {
			// TODO:
			return castExpression;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="reference"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitReference(SqlReferenceExpression reference) {
			return SqlExpression.Reference(reference.ReferenceName);
		}

		public virtual SqlExpression VisitVariableReference(SqlVariableReferenceExpression reference) {
			return SqlExpression.VariableReference(reference.VariableName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="assign"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitAssign(SqlAssignExpression assign) {
			var reference = assign.ReferenceExpression;
			if (reference != null)
				reference = Visit(reference);

			var expression = assign.ValueExpression;
			if (expression != null)
				expression = Visit(expression);

			return SqlExpression.Assign(reference, expression);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="constant"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitConstant(SqlConstantExpression constant) {
			return SqlExpression.Constant(constant.Value);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="conditional"></param>
		/// <returns></returns>
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

		/// <summary>
		/// 
		/// </summary>
		/// <param name="expression"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitTuple(SqlTupleExpression expression) {
			var list = expression.Expressions;
			if (list != null)
				list = VisitExpressionList(list);

			return SqlExpression.Tuple(list);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="query"></param>
		/// <returns></returns>
		public virtual SqlExpression VisitQuery(SqlQueryExpression query) {
			var selectColumns = new List<SelectColumn>();
			foreach (var column in query.SelectColumns) {
				var newColumn = new SelectColumn(Visit(column.Expression), column.Alias);
				newColumn.InternalName = column.InternalName;
				newColumn.ResolvedName = column.ResolvedName;
				selectColumns.Add(newColumn);
			}

			var newQuery = new SqlQueryExpression(selectColumns);

			if (query.FromClause != null)
				newQuery.FromClause = VisitFromClause(query.FromClause);

			if (query.WhereExpression != null)
				newQuery.WhereExpression = Visit(query.WhereExpression);
			if (query.HavingExpression != null)
				newQuery.HavingExpression = Visit(query.HavingExpression);

			if (query.GroupBy != null)
				newQuery.GroupBy = VisitExpressionList(newQuery.GroupBy.ToArray());

			newQuery.GroupMax = query.GroupMax;
			newQuery.Distinct = query.Distinct;

			if (query.NextComposite != null) {
				var visitedComposite = Visit(query.NextComposite);
				if (visitedComposite.ExpressionType == SqlExpressionType.Query)
					newQuery.NextComposite = (SqlQueryExpression) visitedComposite;

				newQuery.CompositeFunction = query.CompositeFunction;
				newQuery.IsCompositeAll = query.IsCompositeAll;
			}

			return query;
		}

		public virtual SqlExpression VisitQuantified(SqlQuantifiedExpression expression) {
			var arg = expression.ValueExpression;
			if (arg != null)
				arg = Visit(arg);

			return SqlExpression.Quantified(expression.ExpressionType, arg);
		}

		private FromClause VisitFromClause(FromClause fromClause) {
			// TODO: 
			return fromClause;
		}
	}
}