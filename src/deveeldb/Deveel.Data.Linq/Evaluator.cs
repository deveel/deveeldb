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
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	static class Evaluator {
		/// <summary>
		/// Performs evaluation & replacement of independent sub-trees
		/// </summary>
		/// <param name="expression">The root of the expression tree.</param>
		/// <param name="fnCanBeEvaluated">A function that decides whether a given expression node can be part of the local function.</param>
		/// <returns>A new tree with sub-trees evaluated and replaced.</returns>
		public static Expression PartialEval(Expression expression, Func<Expression, bool> fnCanBeEvaluated) {
			return new SubtreeEvaluator(new Nominator(fnCanBeEvaluated).Nominate(expression)).Eval(expression);
		}

		/// <summary>
		/// Performs evaluation & replacement of independent sub-trees
		/// </summary>
		/// <param name="expression">The root of the expression tree.</param>
		/// <returns>A new tree with sub-trees evaluated and replaced.</returns>
		public static Expression PartialEval(Expression expression) {
			return PartialEval(expression, CanBeEvaluatedLocally);
		}

		private static bool CanBeEvaluatedLocally(Expression expression) {
			return expression.NodeType != ExpressionType.Parameter;
		}

		/// <summary>
		/// Evaluates & replaces sub-trees when first candidate is reached (top-down)
		/// </summary>
		class SubtreeEvaluator : ExpressionVisitor {
			HashSet<Expression> candidates;

			internal SubtreeEvaluator(HashSet<Expression> candidates) {
				this.candidates = candidates;
			}

			internal Expression Eval(Expression exp) {
				return this.Visit(exp);
			}

			public override Expression Visit(Expression exp) {
				if (exp == null) {
					return null;
				}
				if (candidates.Contains(exp)) {
					return Evaluate(exp);
				}
				return base.Visit(exp);
			}

			private Expression Evaluate(Expression e) {
				if (e.NodeType == ExpressionType.Constant) {
					return e;
				}
				LambdaExpression lambda = Expression.Lambda(e);
				Delegate fn = lambda.Compile();
				return Expression.Constant(fn.DynamicInvoke(null), e.Type);
			}
		}

		/// <summary>
		/// Performs bottom-up analysis to determine which nodes can possibly
		/// be part of an evaluated sub-tree.
		/// </summary>
		class Nominator : ExpressionVisitor {
			Func<Expression, bool> fnCanBeEvaluated;
			HashSet<Expression> candidates;
			bool cannotBeEvaluated;

			internal Nominator(Func<Expression, bool> fnCanBeEvaluated) {
				this.fnCanBeEvaluated = fnCanBeEvaluated;
			}

			internal HashSet<Expression> Nominate(Expression expression) {
				this.candidates = new HashSet<Expression>();
				this.Visit(expression);
				return this.candidates;
			}

			public override Expression Visit(Expression expression) {
				if (expression != null) {
					bool saveCannotBeEvaluated = this.cannotBeEvaluated;
					this.cannotBeEvaluated = false;
					base.Visit(expression);
					if (!this.cannotBeEvaluated) {
						if (this.fnCanBeEvaluated(expression)) {
							this.candidates.Add(expression);
						} else {
							this.cannotBeEvaluated = true;
						}
					}
					this.cannotBeEvaluated |= saveCannotBeEvaluated;
				}
				return expression;
			}
		}
	}
}
