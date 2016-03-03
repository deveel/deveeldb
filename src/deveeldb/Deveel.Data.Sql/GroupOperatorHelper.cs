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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql {
	static class GroupOperatorHelper {
		private static bool IsTrue(Field b) {
			return (!b.IsNull &&
					b.Type is BooleanType &&
					b.Value.Equals(SqlBoolean.True));
		}

		public static Field EvaluateAny(SqlExpressionType plainType, Field ob1, Field ob2, EvaluateContext context) {
			if (ob2.Type is QueryType) {
				// The sub-query plan
				var plan = ((SqlQueryObject)ob2.Value).QueryPlan;
				// Discover the correlated variables for this plan.
				var list = plan.DiscoverQueryReferences(1);

				if (list.Count > 0) {
					// Set the correlated variables from the IVariableResolver
					foreach (var variable in list) {
						variable.Evaluate(context.VariableResolver);
					}

					// Clear the cache in the context
					context.Request.Query.ClearCachedTables();
				}

				// Evaluate the plan,
				var t = plan.Evaluate(context.Request);

				// The ANY operation
				var revPlainOp = plainType.Reverse();
				// TODO: return t.ColumnMatchesValue(0, revPlainOp, ob1);
				throw new NotImplementedException();
			}

			if (ob2.Type is ArrayType) {
				var expList = (SqlArray)ob2.Value;
				// Assume there are no matches
				var retVal = Field.BooleanFalse;
				foreach (var exp in expList) {
					var expItem = exp.Evaluate(context);
					if (expItem.ExpressionType != SqlExpressionType.Constant)
						throw new InvalidOperationException();

					var evalItem = (SqlConstantExpression) expItem;

					// If null value, return null if there isn't otherwise a match found.
					if (evalItem.Value.IsNull) {
						retVal = Field.BooleanNull;
					} else if (IsTrue(Evaluate(ob1, plainType, evalItem.Value, context))) {
						// If there is a match, the ANY set test is true
						return Field.BooleanTrue;
					}
				}
				// No matches, so return either false or NULL.  If there are no matches
				// and no nulls, return false.  If there are no matches and there are
				// nulls present, return null.
				return retVal;
			}

			throw new InvalidOperationException("Unknown RHS of ANY.");
		}

		public static Field EvaluateAll(SqlExpressionType plainType, Field ob1, Field ob2,
			EvaluateContext context) {
			if (ob2.Type is QueryType) {
				// The sub-query plan
				var planObj = (SqlQueryObject) ob2.Value;

				// Discover the correlated variables for this plan.
				var list = planObj.QueryPlan.DiscoverQueryReferences(1);

				if (list.Count > 0) {
					// Set the correlated variables from the IVariableResolver
					foreach (var variable in list) {
						variable.Evaluate(context.VariableResolver);
					}

					// Clear the cache in the context
					context.Request.Query.ClearCachedTables();
				}

				// Evaluate the plan,
				var t = planObj.QueryPlan.Evaluate(context.Request);

				var revPlainOp = plainType.Reverse();
				return Field.Boolean(t.AllRowsMatchColumnValue(0, revPlainOp, ob1));
			}
			if (ob2.Type is ArrayType) {
				var expList = (SqlArray) ob2.Value;

				// Assume true unless otherwise found to be false or NULL.
				Field retVal = Field.BooleanTrue;
				foreach (var exp in expList) {
					var expItem = exp.Evaluate(context);

					if (expItem.ExpressionType != SqlExpressionType.Constant)
						throw new InvalidOperationException();

					var evalItem = (SqlConstantExpression)expItem;

					// If there is a null item, we return null if not otherwise found to
					// be false.
					if (evalItem.Value.IsNull) {
						retVal = Field.BooleanNull;
					} else if (!IsTrue(Evaluate(ob1, plainType, evalItem.Value, context))) {
						// If it doesn't match return false
						return Field.BooleanFalse;
					}
				}

				// Otherwise return true or null.  If all match and no NULLs return
				// true.  If all match and there are NULLs then return NULL.
				return retVal;
			}

			throw new InvalidOperationException("Unknown RHS of ALL.");
		}

		private static Field Evaluate(Field left, SqlExpressionType binaryType, Field right, EvaluateContext context) {
			if (binaryType.IsAll())
				return left.Any(binaryType.SubQueryPlainType(), right, context);
			if (binaryType.IsAny())
				return left.All(binaryType.SubQueryPlainType(), right, context);

			switch (binaryType) {
				case SqlExpressionType.Add:
					return left.Add(right);
				case SqlExpressionType.Subtract:
					return left.Subtract(right);
				case SqlExpressionType.Multiply:
					return left.Multiply(right);
				case SqlExpressionType.Divide:
					return left.Divide(right);
				case SqlExpressionType.Modulo:
					return left.Modulus(right);
				case SqlExpressionType.GreaterThan:
					return left.IsGreaterThan(right);
				case SqlExpressionType.GreaterOrEqualThan:
					return left.IsGreterOrEqualThan(right);
				case SqlExpressionType.SmallerThan:
					return left.IsSmallerThan(right);
				case SqlExpressionType.SmallerOrEqualThan:
					return left.IsSmallerOrEqualThan(right);
				case SqlExpressionType.Equal:
					return left.IsEqualTo(right);
				case SqlExpressionType.NotEqual:
					return left.IsNotEqualTo(right);
				case SqlExpressionType.Is:
					return left.Is(right);
				case SqlExpressionType.IsNot:
					return left.IsNot(right);
				case SqlExpressionType.Like:
					return left.IsLike(right);
				case SqlExpressionType.NotLike:
					return left.IsNotLike(right);
				case SqlExpressionType.And:
					return left.And(right);
				case SqlExpressionType.Or:
					return left.Or(right);
				case SqlExpressionType.XOr:
					return left.XOr(right);
				// TODO: ANY and ALL
				default:
					throw new ExpressionEvaluateException(String.Format("The type {0} is not a binary expression or is not supported.", binaryType));
			}
		}
	}
}
