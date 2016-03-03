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
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// Extension methods to <see cref="SqlExpression"/>
	/// </summary>
	public static class SqlExpressionExtensions {
		/// <summary>
		/// Extracts the name of the reference from the expression.
		/// </summary>
		/// <param name="expression">The expression that encapsulates the reference</param>
		/// <returns>
		/// Returns an <see cref="ObjectName"/> instance if the given <paramref name="expression"/>
		/// is a <see cref="SqlReferenceExpression"/>, otherwise it returns <c>null</c>.
		/// </returns>
		public static ObjectName AsReferenceName(this SqlExpression expression) {
			var refExpression = expression as SqlReferenceExpression;
			if (refExpression == null)
				return null;

			return refExpression.ReferenceName;
		}

		public static IQueryPlanNode AsQueryPlan(this SqlExpression expression) {
			var constantExp = expression as SqlConstantExpression;
			if (constantExp == null)
				return null;

			var value = constantExp.Value;
			if (value.Value is SqlQueryObject)
				return ((SqlQueryObject) value.Value).QueryPlan;

			return null;
		}

		/// <summary>
		/// Evaluates the expression and reduces to a constant expression,
		/// whose value is then returned.
		/// </summary>
		/// <param name="expression">The expression to evaluate.</param>
		/// <param name="context">The context used to evaluate the expression.</param>
		/// <returns>
		/// Returns a <see cref="Field"/> is the result of the
		/// <see cref="SqlExpression.Evaluate(Deveel.Data.Sql.Expressions.EvaluateContext)"/>
		/// is a <see cref="SqlConstantExpression"/>.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the expression could not be evaluated or if the result of the
		/// evaluation is not a <see cref="SqlConstantExpression"/>.
		/// </exception>
		/// <seealso cref="SqlExpression.Evaluate(Deveel.Data.Sql.Expressions.EvaluateContext)"/>
		/// <seealso cref="SqlConstantExpression"/>
		/// <seealso cref="SqlConstantExpression.Value"/>
		public static Field EvaluateToConstant(this SqlExpression expression, EvaluateContext context) {
			var evalExp = expression.Evaluate(context);
			if (evalExp == null)
				throw new InvalidOperationException();

			var constantExp = evalExp as SqlConstantExpression;
			if (constantExp == null)
				throw new InvalidOperationException();

			return constantExp.Value;
		}

		public static Field EvaluateToConstant(this SqlExpression expression, IRequest request, IVariableResolver variableResolver) {
			return expression.EvaluateToConstant(new EvaluateContext(request, variableResolver));
		}

		/// <summary>
		/// Gets the return type of the expression when evaluated.
		/// </summary>
		/// <param name="expression">The expression to check.</param>
		/// <param name="query">The query context used to evaluate the return type
		/// of the expression.</param>
		/// <param name="variableResolver">The object used to resolve variable references in the expression tree.</param>
		/// <returns>
		/// Returns the <see cref="SqlType"/> that an evaluation of the expression
		/// would return, or <c>null</c> if the final result of the evaluation has
		/// no return type.
		/// </returns>
		public static SqlType ReturnType(this SqlExpression expression, IRequest query, IVariableResolver variableResolver) {
			var visitor = new ReturnTypeVisitor(query, variableResolver);
			return visitor.GetType(expression);
		}

		/// <summary>
		/// Verifies if the expression contains any aggregate function
		/// in the tree.
		/// </summary>
		/// <param name="expression">The expression to verify.</param>
		/// <param name="query"></param>
		/// <returns>
		/// Returns <c>true</c> if the expression has any aggregate function in its tree,
		/// or <c>false</c> otherwise.
		/// </returns>
		public static bool HasAggregate(this SqlExpression expression, IRequest query) {
			var visitor = new AggregateChecker(query);
			return visitor.HasAggregate(expression);
		}

		public static bool IsConstant(this SqlExpression expression) {
			var visitor = new ConstantVisitor();
			visitor.Visit(expression);
			return visitor.IsConstant;
		}
	}
}
