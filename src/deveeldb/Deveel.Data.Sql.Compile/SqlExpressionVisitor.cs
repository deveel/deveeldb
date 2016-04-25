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
using System.Globalization;
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Compile {
	class SqlExpressionVisitor : PlSqlParserBaseVisitor<SqlExpression> {
		public override SqlExpression VisitAtom(PlSqlParser.AtomContext context) {
			var subquery = context.subquery();
			if (subquery != null && !subquery.IsEmpty)
				return Visit(subquery);

			var constant = context.constant();
			if (constant != null && !constant.IsEmpty)
				return Visit(constant);

			var bindVariable = context.bind_variable();
			if (bindVariable != null && !bindVariable.IsEmpty)
				return Visit(bindVariable);

			var group = context.group();
			if (group != null && !group.IsEmpty)
				return Visit(group);

			return base.VisitAtom(context);
		}

		public override SqlExpression VisitExpression_unit(PlSqlParser.Expression_unitContext context) {
			return Visit(context.expression());
		}

		public override SqlExpression VisitUnaryplus_expression(PlSqlParser.Unaryplus_expressionContext context) {
			return SqlExpression.UnaryPlus(Visit(context.unary_expression()));
		}

		public override SqlExpression VisitUnaryminus_expression(PlSqlParser.Unaryminus_expressionContext context) {
			return SqlExpression.Unary(SqlExpressionType.Negate, Visit(context.unary_expression()));
		}

		private SqlExpressionType GetBinaryOperator(string s) {
			if (s == "+")
				return SqlExpressionType.Add;
			if (s == "-")
				return SqlExpressionType.Subtract;
			if (s == "=")
				return SqlExpressionType.Equal;
			if (s == "<>" || s == "!=")
				return SqlExpressionType.NotEqual;
			if (s == "/")
				return SqlExpressionType.Divide;
			if (s == "*")
				return SqlExpressionType.Multiply;
			if (s == "%" ||
				String.Equals(s, "MOD", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Modulo;
			if (s == ">")
				return SqlExpressionType.GreaterThan;
			if (s == ">=")
				return SqlExpressionType.GreaterOrEqualThan;
			if (s == "<")
				return SqlExpressionType.SmallerThan;
			if (s == "<=")
				return SqlExpressionType.SmallerOrEqualThan;

			if (String.Equals(s, "IS", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Is;
			if (String.Equals(s, "IS NOT", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.IsNot;

			if (String.Equals(s, "LIKE", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Like;
			if (String.Equals(s, "NOT LIKE", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.NotLike;

			throw new NotSupportedException(String.Format("Expression type '{0}' not supported", s));
		}

		public override SqlExpression VisitRelational_expression(PlSqlParser.Relational_expressionContext context) {
			var left = Visit(context.left);

			string op;
			var neOp = context.notEqual();
			var geOp = context.greaterThanOrEquals();
			var leOp = context.lessThanOrEquals();
			if (context.op != null) {
				op = context.op.Text;
			} else if (neOp.Length > 0) {
				op = String.Join(" ", neOp.Select(x => x.GetText()).ToArray());
			} else if (geOp.Length > 0) {
				op = String.Join(" ", geOp.Select(x => x.GetText()).ToArray());
			} else if (leOp.Length > 0) {
				op = String.Join(" ", leOp.Select(x => x.GetText()).ToArray());
			} else {
				return left;
			}

			var expType = GetBinaryOperator(op);
			var right = Visit(context.right);

			return SqlExpression.Binary(left, expType, right);
		}

		public override SqlExpression VisitSimpleCaseStatement(PlSqlParser.SimpleCaseStatementContext context) {
			return base.VisitSimpleCaseStatement(context);
		}

		public override SqlExpression VisitSearchedCaseStatement(PlSqlParser.SearchedCaseStatementContext context) {
			return base.VisitSearchedCaseStatement(context);
		}

		public override SqlExpression VisitSubquery(PlSqlParser.SubqueryContext context) {
			return Subquery.Form(context);
		}

		public override SqlExpression VisitQuantifiedExpression(PlSqlParser.QuantifiedExpressionContext context) {
			SqlExpression arg;
			if (context.subquery() != null) {
				arg = Subquery.Form(context.subquery());
			} else if (context.expression_list() != null) {
				var elements = context.expression_list().expression().Select(Visit).ToArray();
				arg = SqlExpression.Constant(Field.Array(elements));
			} else {
				throw new ParseCanceledException("Invalid argument in a quantified expression.");
			}

			if (context.EXISTS() != null) {
				if (!(arg is SqlQueryExpression))
					throw new ParseCanceledException("The EXISTS function can be evaluated only against a sub-query.");

				return SqlExpression.FunctionCall("EXISTS", new[] {arg});
			}
			if (context.ALL() != null) {
				return SqlExpression.All(arg);
			}
			if (context.ANY() != null ||
			    context.SOME() != null) {
				return SqlExpression.Any(arg);
			}

			return base.VisitQuantifiedExpression(context);
		}

		public override SqlExpression VisitExpression_or_vector(PlSqlParser.Expression_or_vectorContext context) {
			var exp = Visit(context.expression());
			var vector = context.vector_expr();
			if (vector == null)
				return exp;

			var exps = vector.expression().Select(Visit).ToArray();
			if (exps.Length == 0)
				return exp;

			var array = new SqlExpression[exps.Length + 1];
			array[0] = exp;
			Array.Copy(exps, 0, array, 1, exps.Length);

			return SqlExpression.Constant(Field.Array(array));
		}

		public override SqlExpression VisitEquality_expression(PlSqlParser.Equality_expressionContext context) {
			var left = Visit(context.relational_expression());

			string op = null;

			if (context.IS().Length > 0) {
				if (context.NOT().Length > 0) {
					op = "IS NOT";
				} else {
					op = "IS";
				}
			}

			if (String.IsNullOrEmpty(op))
				return left;

			object value;
			if (context.EMPTY().Length > 0) {
				value = String.Empty;
			} else if (context.NAN().Length > 0) {
				value = Double.NaN;
			} else if (context.NULL().Length > 0) {
				value = null;
			} else if (context.OF().Length > 0) {
				// TODO: return TYPEOF function
				throw new NotImplementedException();
			} else {
				throw new NotSupportedException();
			}

			var right = SqlExpression.Constant(value);
			var expType = GetBinaryOperator(op);
			return SqlExpression.Binary(left, expType, right);
		}

		public override SqlExpression VisitAdditive_expression(PlSqlParser.Additive_expressionContext context) {
			var left = Visit(context.left);

			if (context.right == null)
				return left;

			var right = Visit(context.right);
			var op = context.op.Text;

			var expType = GetBinaryOperator(op);
			return SqlExpression.Binary(left, expType, right);
		}

		public override SqlExpression VisitLogical_and_expression(PlSqlParser.Logical_and_expressionContext context) {
			var exps = context.negated_expression().Select(Visit).ToArray();
			if (exps.Length == 1)
				return exps[0];

			return SqlExpression.And(exps[0], exps[1]);
		}

		public override SqlExpression VisitExpression(PlSqlParser.ExpressionContext context) {
			var left = Visit(context.left);

			if (context.right == null)
				return left;

			var right = Visit(context.right);
			return SqlExpression.Or(left, right);
		}

		public override SqlExpression VisitNegated_expression(PlSqlParser.Negated_expressionContext context) {
			if (context.NOT() != null) {
				var eqExp = Visit(context.negated_expression());
				return SqlExpression.Not(eqExp);
			}

			return Visit(context.equality_expression());
		}

		public override SqlExpression VisitMultiply_expression(PlSqlParser.Multiply_expressionContext context) {
			var left = Visit(context.left);

			if (context.right == null)
				return left;

			var right = Visit(context.right);
			var op = context.op.Text;

			var expType = GetBinaryOperator(op);

			return SqlExpression.Binary(left, expType, right);
		}

		public override SqlExpression VisitConstantDBTimeZone(PlSqlParser.ConstantDBTimeZoneContext context) {
			return SqlExpression.FunctionCall("DBTIMEZONE");
		}

		public override SqlExpression VisitConstantFalse(PlSqlParser.ConstantFalseContext context) {
			return SqlExpression.Constant(Field.BooleanFalse);
		}

		public override SqlExpression VisitConstantTrue(PlSqlParser.ConstantTrueContext context) {
			return SqlExpression.Constant(Field.BooleanTrue);
		}

		public override SqlExpression VisitConstantNull(PlSqlParser.ConstantNullContext context) {
			return SqlExpression.Constant(Field.Null());
		}

		public override SqlExpression VisitConstantNumeric(PlSqlParser.ConstantNumericContext context) {
			var value = context.numeric().GetText();
			var formatInfo = new NumberFormatInfo {
				NumberDecimalSeparator = "."
			};

			var dValue = Double.Parse(value,formatInfo);

			return SqlExpression.Constant(Field.Number(new SqlNumber(dValue)));
		}

		public override SqlExpression VisitConstantString(PlSqlParser.ConstantStringContext context) {
			var value = InputString.AsNotQuoted(context.quoted_string());
			return SqlExpression.Constant(Field.String(value));
		}

		private ObjectName FormName(string[] parts) {
			if (parts == null || parts.Length == 0)
				return null;

			ObjectName result = null;

			for (int i = 0; i < parts.Length; i++) {
				if (result == null) {
					result = new ObjectName(parts[i]);
				} else {
					result = new ObjectName(result, parts[i]);
				}
			}

			return result;
		}

		public override SqlExpression VisitGeneral_element(PlSqlParser.General_elementContext context) {
			var element = ElementNode.Form(context);
			var name = element.Id;

			if (element.Argument == null ||
				element.Argument.Length == 0)
				return SqlExpression.Reference(name);

			// TODO: support argument naming in DeveelDB
			var funcArgs = element.Argument.Select(x => x.Expression).ToArray();
			return SqlExpression.FunctionCall(name, funcArgs);
		}

		public override SqlExpression VisitCompound_expression(PlSqlParser.Compound_expressionContext context) {
			var left = Visit(context.exp);

			bool isNot = context.NOT() != null;
			if (context.LIKE() != null) {
				var right = Visit(context.likeExp);
				var op = isNot ? SqlExpressionType.NotLike : SqlExpressionType.Like;
				return SqlExpression.Binary(left, op, right);
			}
			if (context.BETWEEN() != null) {
				var min = Visit(context.min);
				var max = Visit(context.max);
				var lowerBound = SqlExpression.GreaterOrEqualThan(left, min);
				var upperBound = SqlExpression.SmallerOrEqualThan(left, max);
				return SqlExpression.And(lowerBound, upperBound);
			}

			if (context.IN() != null) {
				var arg = Visit(context.in_elements());

				SqlExpression right;
				SqlExpressionType op;

				if (isNot) {
					right = SqlExpression.All(arg);
					op = SqlExpressionType.NotEqual;
				} else {
					right = SqlExpression.Any(arg);
					op = SqlExpressionType.Equal;
				}

				return SqlExpression.Binary(left, op, right);
			}

			return left;
		}

		public override SqlExpression VisitInArray(PlSqlParser.InArrayContext context) {
			var exps = context.concatenation_wrapper().Select(Visit);
			return SqlExpression.Constant(Field.Array(exps));
		}

		public override SqlExpression VisitInSubquery(PlSqlParser.InSubqueryContext context) {
			return Visit(context.subquery());
		}

		public override SqlExpression VisitInConstant(PlSqlParser.InConstantContext context) {
			return Visit(context.constant());
		}

		public override SqlExpression VisitInVariable(PlSqlParser.InVariableContext context) {
			return Visit(context.bind_variable());
		}

		public override SqlExpression VisitGroup(PlSqlParser.GroupContext context) {
			var exp = Visit(context.expression_or_vector());
			return SqlExpression.Tuple(new[] {exp});
		}

		public override SqlExpression VisitDatetime_expression(PlSqlParser.Datetime_expressionContext context) {
			var exp = Visit(context.unary_expression());
			if (context.AT() == null)
				return exp;

			throw new NotImplementedException();
		}

		public override SqlExpression VisitConcatenation(PlSqlParser.ConcatenationContext context) {
			var left = Visit(context.left);
			if (context.right == null)
				return left;

			var right = Visit(context.right);
			var op = context.op.GetText();
			var expType = GetBinaryOperator(op);

			return SqlExpression.Binary(left, expType, right);
		}

		public override SqlExpression VisitBind_variable(PlSqlParser.Bind_variableContext context) {
			string varRef;
			if (context.BINDVAR() != null) {
				varRef = context.BINDVAR().GetText();
			} else if (context.UNSIGNED_INTEGER() != null) {
				var numVal = context.UNSIGNED_INTEGER().GetText();
				varRef = String.Format(":{0}", numVal);
			} else {
				throw new ParseCanceledException("Invalid variable bind");
			}

			// TODO: support more complex variable binds

			return SqlExpression.VariableReference(varRef);
		}

		public override SqlExpression VisitExpression_wrapper(PlSqlParser.Expression_wrapperContext context) {
			return Visit(context.expression());
		}

		public override SqlExpression VisitCondition(PlSqlParser.ConditionContext context) {
			return Visit(context.expression());
		}

		public override SqlExpression VisitCastFunction(PlSqlParser.CastFunctionContext context) {
			if (context.MULTISET() != null)
				throw new NotImplementedException();

			var destType = SqlTypeParser.Parse(context.datatype());
			var destTypeString = destType.ToString();

			var value = Visit(context.concatenation_wrapper());

			return SqlExpression.FunctionCall("SQL_CAST", new[] {value, SqlExpression.Constant(destTypeString)});
		}

		public override SqlExpression VisitInvokedFunction(PlSqlParser.InvokedFunctionContext context) {
			var name = Name.Object(context.objectName());
			InvokeArgument[] args = null;

			if (context.argument() != null) {
				args = context.argument().Select(FunctionArgument.Form).Select(x => new InvokeArgument(x.Id, x.Expression)).ToArray();
			}

			return SqlExpression.FunctionCall(name, args);
		}

		public override SqlExpression VisitCountFunction(PlSqlParser.CountFunctionContext context) {
			if (context.all != null)
				return SqlExpression.FunctionCall("COUNT", new SqlExpression[] {SqlExpression.Reference(new ObjectName("*"))});

			var exp = Visit(context.concatenation_wrapper());
			if (context.DISTINCT() != null)
				return SqlExpression.FunctionCall("DISTINCT_COUNT", new[] {exp});				
			if (context.UNIQUE() != null)
				return SqlExpression.FunctionCall("UNIQUE_COUNT", new[] {exp});

			return SqlExpression.FunctionCall("COUNT", new[] { exp });
		}

		public override SqlExpression VisitExtractFunction(PlSqlParser.ExtractFunctionContext context) {
			var part = Name.Simple(context.regular_id());
			var exp = Visit(context.concatenation_wrapper());

			return SqlExpression.FunctionCall("SQL_EXTRACT", new[] {exp, SqlExpression.Constant(part)});
		}

		public override SqlExpression VisitNextValueFunction(PlSqlParser.NextValueFunctionContext context) {
			var seqName = Name.Object(context.objectName());
			return SqlExpression.FunctionCall("NEXTVAL", new SqlExpression[] {SqlExpression.Constant(seqName.ToString())});
		}

		public override SqlExpression VisitCurrentTimeFunction(PlSqlParser.CurrentTimeFunctionContext context) {
			return SqlExpression.FunctionCall("TIME");
		}

		public override SqlExpression VisitCurrentTimeStampFunction(PlSqlParser.CurrentTimeStampFunctionContext context) {
			return SqlExpression.FunctionCall("TIMESTAMP");
		}

		public override SqlExpression VisitCurrentDateFunction(PlSqlParser.CurrentDateFunctionContext context) {
			return SqlExpression.FunctionCall("DATE");
		}

		public override SqlExpression VisitTrimFunction(PlSqlParser.TrimFunctionContext context) {
			return base.VisitTrimFunction(context);
		}

		public override SqlExpression VisitTreatFunction(PlSqlParser.TreatFunctionContext context) {
			return base.VisitTreatFunction(context);
		}
	}
}
