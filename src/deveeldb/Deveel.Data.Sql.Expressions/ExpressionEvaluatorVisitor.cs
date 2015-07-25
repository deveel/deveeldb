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
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	class ExpressionEvaluatorVisitor : SqlExpressionVisitor {
		private readonly EvaluateContext context;

		public ExpressionEvaluatorVisitor(EvaluateContext context) {
			this.context = context;
		}

		public override SqlExpression Visit(SqlExpression expression) {
			if (expression is QueryReferenceExpression)
				return VisitQueryReference((QueryReferenceExpression) expression);

			return base.Visit(expression);
		}

		private SqlExpression VisitQueryReference(QueryReferenceExpression expression) {
			var reference = expression.Reference;
			var value = reference.Evaluate(context.VariableResolver);
			return SqlExpression.Constant(value);
		}

		private SqlExpression[] EvaluateSides(SqlBinaryExpression binary) {
			var info = new List<BinaryEvaluateInfo> {
				new BinaryEvaluateInfo {Expression = binary.Left, Offset = 0},
				new BinaryEvaluateInfo {Expression = binary.Right, Offset = 1}
			}.OrderByDescending(x => x.Precedence);

			foreach (var evaluateInfo in info) {
				evaluateInfo.Expression = Visit(evaluateInfo.Expression);
			}

			return info.OrderBy(x => x.Offset)
				.Select(x => x.Expression)
				.ToArray();
		}

		private SqlExpression EvaluateBinary(SqlExpression left, SqlExpressionType binaryType, SqlExpression right) {
			if (left.ExpressionType != SqlExpressionType.Constant)
				throw new ExpressionEvaluateException("The evaluated left side of a binary expression is not constant");
			if (right.ExpressionType != SqlExpressionType.Constant)
				throw new ExpressionEvaluateException("The evaluated right side of a binary expression is not constant.");

			var value1 = ((SqlConstantExpression) left).Value;
			var value2 = ((SqlConstantExpression) right).Value;

			var result = EvaluateBinary(value1, binaryType, value2);

			return SqlExpression.Constant(result);
		}

		private DataObject EvaluateBinary(DataObject left, SqlExpressionType binaryType, DataObject right) {
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

		public override SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
			var sides = EvaluateSides(binaryEpression);

			var newLeft = sides[0];
			var newRight = sides[1];

			return EvaluateBinary(newLeft, binaryEpression.ExpressionType, newRight);
		}

		public override SqlExpression VisitCast(SqlCastExpression castExpression) {
			var valueExp = Visit(castExpression.Value);
			if (valueExp.ExpressionType != SqlExpressionType.Constant)
				throw new ExpressionEvaluateException(String.Format("Cannot CAST an expression of type {0}.", valueExp.ExpressionType));

			var value = ((SqlConstantExpression) valueExp).Value;
			var casted = value.CastTo(castExpression.DataType);
			return SqlExpression.Constant(casted);
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (value.IsNull)
				return constant;

			var obj = value.Value;
			if (obj is SqlQueryObject) {
				return EvaluateQueryPlan((SqlQueryObject) obj);
			}

			return base.VisitConstant(constant);
		}

		private SqlConstantExpression EvaluateQueryPlan(SqlQueryObject obj) {
			if (context.QueryContext == null)
				throw new ExpressionEvaluateException("A query context is required to evaluate a query.");

			try {
				var plan = obj.QueryPlan;
				var result = plan.Evaluate(context.QueryContext);

				return SqlExpression.Constant(new DataObject(new TabularType(), SqlTabular.From(result)));
			} catch (ExpressionEvaluateException) {
				throw;
			} catch (Exception ex) {
				throw new ExpressionEvaluateException("Could not evaluate a query.", ex);
			}
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			try {
				var invoke = new Invoke(expression.FunctioName, expression.Arguments);
				IQueryContext queryContext = null;
				IVariableResolver variableResolver = null;
				IGroupResolver groupResolver = null;
				if (context != null) {
					queryContext = context.QueryContext;
					variableResolver = context.VariableResolver;
					groupResolver = context.GroupResolver;
				}

				// TODO: if we don't have a return value (PROCEDURES) what should w return?
				var result = invoke.Execute(queryContext, variableResolver, groupResolver);
				if (!result.HasReturnValue)
					return SqlExpression.Constant(DataObject.Null());

				return SqlExpression.Constant(result.ReturnValue);
			} catch (ExpressionEvaluateException) {
				throw;
			} catch (Exception ex) {
				throw new ExpressionEvaluateException(String.Format("Could not evaluate function expression '{0}' because of an error.", expression), ex);
			}
		}

		public override SqlExpression VisitQuery(SqlQueryExpression query) {
			if (context.DatabaseContext == null)
				throw new ExpressionEvaluateException("A database context is required to evaluate a query.");
			if (context.QueryContext == null)
				throw new ExpressionEvaluateException("A query expression is required to evaluate a query.");

			try {
				var planner = context.DatabaseContext.QueryPlanner();
				var plan = planner.PlanQuery(context.QueryContext, query, null);
				return SqlExpression.Constant(new DataObject(new QueryType(), new SqlQueryObject(plan)));
			} catch (ExpressionEvaluateException) {
				throw;
			} catch (Exception ex) {
				throw new ExpressionEvaluateException("Evaluation of a QUERY expression could not generate a plan.", ex);
			}
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			var refName = reference.ReferenceName;

			if (context.VariableResolver == null)
				throw new ExpressionEvaluateException(String.Format("A resolver is required to dereference variable '{0}'.", refName));

			try {
				var value = context.VariableResolver.Resolve(refName);
				return SqlExpression.Constant(value);
			} catch (ExpressionEvaluateException) {
				throw;
			} catch (Exception ex) {
				throw new ExpressionEvaluateException(String.Format("An error occurred while trying to dereference '{0}' to a constant value", refName), ex);
			}
		}

		public override SqlExpression VisitUnary(SqlUnaryExpression unary) {
			var operand = Visit(unary.Operand);
			if (operand.ExpressionType != SqlExpressionType.Constant)
				throw new ExpressionEvaluateException("Operand of a unary operator could not be evaluated to a constant.");

			var result = EvaluateUnary(((SqlConstantExpression)operand).Value, unary.ExpressionType);
			return SqlExpression.Constant(result);
		}

		private DataObject EvaluateUnary(DataObject operand, SqlExpressionType unaryType) {
			switch (unaryType) {
				case SqlExpressionType.UnaryPlus:
					return operand.Plus();
				case SqlExpressionType.Negate:
				case SqlExpressionType.Not:
					return operand.Negate();
				default:
					throw new ExpressionEvaluateException(String.Format("Expression of type '{0}' is not unary.", unaryType));
			}
		}

		public override SqlExpression VisitAssign(SqlAssignExpression assign) {
			if (context.QueryContext == null)
				throw new ExpressionEvaluateException("Cannot assign a variable outside a query context.");

			var valueExpression = Visit(assign.ValueExpression);

			if (valueExpression.ExpressionType != SqlExpressionType.Constant)
				throw new ExpressionEvaluateException("Cannot assign a variable from a non constant value.");

			var reference = assign.Reference;
			var value = ((SqlConstantExpression)valueExpression).Value;

			if (reference is SqlVariableReferenceExpression) {
				try {
					var variable = ((SqlVariableReferenceExpression) reference).VariableName;
					context.QueryContext.SetVariable(variable, value);
				} catch (Exception ex) {
					throw new ExpressionEvaluateException(String.Format("Could not assign value to variable '{0}'", reference), ex);
				}
			} else {
				throw new NotImplementedException();
			}

			return SqlExpression.Constant(value);
		}

		public override SqlExpression VisitTuple(SqlTupleExpression expression) {
			var list = expression.Expressions;
			if (list != null) {
				list = VisitExpressionList(list);
			}

			if (list == null)
				return SqlExpression.Constant(new DataObject(new ArrayType(-1), SqlArray.Null));
				
			return SqlExpression.Constant(new DataObject(new ArrayType(list.Length), new SqlArray(list)));
		}

		public override SqlExpression VisitVariableReference(SqlVariableReferenceExpression reference) {
			var refName = reference.VariableName;

			if (context.QueryContext == null)
				throw new ExpressionEvaluateException(String.Format("Cannot dereference variable {0} outside a query context", refName));
			if (context.VariableResolver == null)
				throw new ExpressionEvaluateException("The query context does not handle variables.");

			
			var variable = context.QueryContext.GetVariable(refName);
			if (variable == null)
				return SqlExpression.Constant(DataObject.Null());

			return base.VisitVariableReference(reference);
		}

		public override SqlExpression VisitConditional(SqlConditionalExpression conditional) {
			var evalTest = Visit(conditional.TestExpression);
			if (evalTest.ExpressionType != SqlExpressionType.Constant)
				throw new ExpressionEvaluateException("The test expression of a conditional must evaluate to a constant value.");

			var evalTestValue = ((SqlConstantExpression) evalTest).Value;
			if (!(evalTestValue.Type is BooleanType))
				throw new ExpressionEvaluateException("The test expression of a conditional must be a boolean value.");

			if (evalTestValue)
				return Visit(conditional.TrueExpression);

			if (conditional.FalseExpression != null)
				return Visit(conditional.FalseExpression);

			return SqlExpression.Constant(PrimitiveTypes.Null());
		}

		#region BinaryEvaluateInfo

		class BinaryEvaluateInfo {
			public SqlExpression Expression { get; set; }
			public int Offset { get; set; }

			public int Precedence {
				get { return Expression.EvaluatePrecedence; }
			}
		}

		#endregion
	}
}
