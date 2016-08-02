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
using System.Globalization;
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

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

		public override SqlExpression VisitExpressionUnit(PlSqlParser.ExpressionUnitContext context) {
			return Visit(context.expression());
		}

		public override SqlExpression VisitUnaryplusExpression(PlSqlParser.UnaryplusExpressionContext context) {
			return SqlExpression.UnaryPlus(Visit(context.unaryExpression()));
		}

		public override SqlExpression VisitUnaryminusExpression(PlSqlParser.UnaryminusExpressionContext context) {
			return SqlExpression.Unary(SqlExpressionType.Negate, Visit(context.unaryExpression()));
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

		public override SqlExpression VisitRelationalExpression(PlSqlParser.RelationalExpressionContext context) {
			var exps = context.compoundExpression().Select(Visit).ToArray();

			SqlExpression last = null;
			for (int i = 0; i < exps.Length; i++) {
				if (last == null) {
					last = exps[i];
				} else {
					var opContext = context.relationalOperator(i-1);
					SqlExpressionType expType;
					if (opContext.greaterThanOrEquals() != null) {
						expType = SqlExpressionType.GreaterOrEqualThan;
					} else if (opContext.lessThanOrEquals() != null) {
						expType = SqlExpressionType.SmallerOrEqualThan;
					} else if (opContext.notEqual() != null) {
						expType = SqlExpressionType.NotEqual;
					} else if (opContext.op != null) {
						expType = GetBinaryOperator(opContext.op.Text);
					} else {
						throw new ParseCanceledException("Invalid relational operator");
					}

					last = SqlExpression.Binary(last, expType, exps[i]);
				}
			}

			return last;
		}

		public override SqlExpression VisitSimpleCaseExpression(PlSqlParser.SimpleCaseExpressionContext context) {
			var exp = Visit(context.atom());

			var switches = new List<CaseSwitch>();

			foreach (var partContext in context.simpleCaseWhenExpressionPart()) {
				var otherExp = Visit(partContext.conditionWrapper());
				switches.Add(new CaseSwitch {
					Condition = SqlExpression.Equal(exp, otherExp),
					ReturnExpression = Visit(partContext.expressionWrapper())
				});
			}

			if (context.caseElseExpressionPart() != null) {
				var returnExp = Visit(context.caseElseExpressionPart().expressionWrapper());
				switches.Add(new CaseSwitch {
					Condition = SqlExpression.Constant(true),
					ReturnExpression = returnExp
				});
			}

			SqlConditionalExpression conditional = null;

			for (int i = switches.Count - 1; i >= 0; i--) {
				var current = switches[i];

				var condition = SqlExpression.Conditional(current.Condition, current.ReturnExpression);

				if (conditional != null) {
					conditional = SqlExpression.Conditional(current.Condition, current.ReturnExpression, conditional);
				} else {
					conditional = condition;
				}
			}

			return conditional;
		}

		public override SqlExpression VisitSearchedCaseExpression(PlSqlParser.SearchedCaseExpressionContext context) {
			var switches = new List<CaseSwitch>();

			foreach (var partContext in context.simpleCaseWhenExpressionPart()) {
				switches.Add(new CaseSwitch {
					Condition = Visit(partContext.conditionWrapper()),
					ReturnExpression = Visit(partContext.expressionWrapper())
				});
			}

			if (context.caseElseExpressionPart() != null) {
				var returnExp = Visit(context.caseElseExpressionPart().expressionWrapper());
				switches.Add(new CaseSwitch {
					Condition = SqlExpression.Conditional(SqlExpression.Constant(true), returnExp),
					ReturnExpression = returnExp
				});
			}

			SqlConditionalExpression conditional = null;

			for (int i = switches.Count - 1; i >= 0; i--) {
				var current = switches[i];

				var condition = SqlExpression.Conditional(current.Condition, current.ReturnExpression);

				if (conditional != null) {
					conditional = SqlExpression.Conditional(current.Condition, current.ReturnExpression, conditional);
				} else {
					conditional = condition;
				}
			}

			return conditional;
		}

		#region CaseSwitch

		class CaseSwitch {
			public SqlExpression Condition { get; set; }

			public SqlExpression ReturnExpression { get; set; }
		}

		#endregion

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

		public override SqlExpression VisitExpressionOrVector(PlSqlParser.ExpressionOrVectorContext context) {
			var exp = Visit(context.expression());
			var vector = context.vectorExpression();
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

		public override SqlExpression VisitEqualityExpression(PlSqlParser.EqualityExpressionContext context) {
			var left = Visit(context.relationalExpression());

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

		public override SqlExpression VisitAdditiveExpression(PlSqlParser.AdditiveExpressionContext context) {
			var exps = context.multiplyExpression().Select(Visit).ToArray();
			if (exps.Length == 1)
				return exps[0];

			SqlExpression last = null;
			for (int i = 0; i < exps.Length; i++) {
				if (last == null) {
					last = exps[i];
				} else {
					var opContext = context.additiveOperator(i - 1);
					var expType = GetBinaryOperator(opContext.GetText());
					last = SqlExpression.Binary(last, expType, exps[i]);
				}
			}

			return last;
		}

		public override SqlExpression VisitLogicalAndExpression(PlSqlParser.LogicalAndExpressionContext context) {
			var exps = context.negatedExpression().Select(Visit).ToArray();
			if (exps.Length == 1)
				return exps[0];

			SqlExpression last = null;
			foreach (var exp in exps) {
				if (last == null) {
					last = exp;
				} else {
					last = SqlExpression.And(last, exp);
				}
			}

			return last;
		}

		public override SqlExpression VisitExpression(PlSqlParser.ExpressionContext context) {
			var exps = context.logicalAndExpression().Select(Visit).ToArray();
			if (exps.Length == 1)
				return exps[0];

			SqlExpression last = null;
			foreach (var exp in exps) {
				if (last == null) {
					last = exp;
				} else {
					last = SqlExpression.Or(last, exp);
				}
			}

			return last;
		}

		public override SqlExpression VisitNegatedExpression(PlSqlParser.NegatedExpressionContext context) {
			if (context.NOT() != null) {
				var eqExp = Visit(context.negatedExpression());
				return SqlExpression.Not(eqExp);
			}

			return Visit(context.equalityExpression());
		}

		public override SqlExpression VisitMultiplyExpression(PlSqlParser.MultiplyExpressionContext context) {
			var exps = context.unaryExpression().Select(Visit).ToArray();
			if (exps.Length == 1)
				return exps[0];

			SqlExpression last = null;
			for (int i = 0; i < exps.Length; i++) {
				if (last == null) {
					last = exps[i];
				} else {
					var opContext = context.multiplyOperator(i - 1);
					var expType = GetBinaryOperator(opContext.GetText());
					last = SqlExpression.Binary(last, expType, exps[i]);
				}
			}

			return last;
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

		public override SqlExpression VisitDateImplicitConvert(PlSqlParser.DateImplicitConvertContext context) {
			var s = InputString.AsNotQuoted(context.quoted_string());
			return SqlExpression.FunctionCall("CAST", new SqlExpression[] {
				SqlExpression.Constant(s),
				SqlExpression.Constant("DATE")
			});
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

		public override SqlExpression VisitCompoundExpression(PlSqlParser.CompoundExpressionContext context) {
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
				var arg = Visit(context.inElements());

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
			var exps = context.concatenationWrapper().Select(Visit);
			return SqlExpression.Constant(Field.Array(exps));
		}

		public override SqlExpression VisitInSubquery(PlSqlParser.InSubqueryContext context) {
			return Visit(context.subquery());
		}

		public override SqlExpression VisitInVariable(PlSqlParser.InVariableContext context) {
			return Visit(context.bind_variable());
		}

		public override SqlExpression VisitGroup(PlSqlParser.GroupContext context) {
			var exp = Visit(context.expressionOrVector());
			if (exp.ExpressionType == SqlExpressionType.Constant &&
			    ((SqlConstantExpression) exp).Value.Type is ArrayType)
				return exp;

			return SqlExpression.Tuple(new[] {exp});
		}

		public override SqlExpression VisitAllExpression(PlSqlParser.AllExpressionContext context) {
			return SqlExpression.All(Visit(context.unaryExpression()));
		}

		public override SqlExpression VisitAnyExpression(PlSqlParser.AnyExpressionContext context) {
			return SqlExpression.Any(Visit(context.unaryExpression()));
		}

		public override SqlExpression VisitConcatenation(PlSqlParser.ConcatenationContext context) {
			var exps = context.additiveExpression().Select(Visit).ToArray();
			if (exps.Length == 1)
				return exps[0];

			return SqlExpression.FunctionCall("CONCAT", exps);
		}

		public override SqlExpression VisitBind_variable(PlSqlParser.Bind_variableContext context) {
			var varRef = Name.Variable(context);
			return SqlExpression.VariableReference(varRef);
		}

		public override SqlExpression VisitExpressionWrapper(PlSqlParser.ExpressionWrapperContext context) {
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

			var value = Visit(context.concatenationWrapper());

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

			var exp = Visit(context.concatenationWrapper());
			if (context.DISTINCT() != null)
				return SqlExpression.FunctionCall("DISTINCT_COUNT", new[] {exp});				
			if (context.UNIQUE() != null)
				return SqlExpression.FunctionCall("UNIQUE_COUNT", new[] {exp});

			return SqlExpression.FunctionCall("COUNT", new[] { exp });
		}

		public override SqlExpression VisitExtractFunction(PlSqlParser.ExtractFunctionContext context) {
			var part = Name.Simple(context.regular_id());
			var exp = Visit(context.concatenationWrapper());

			return SqlExpression.FunctionCall("SQL_EXTRACT", new[] {exp, SqlExpression.Constant(part)});
		}

		public override SqlExpression VisitTimeStampFunction(PlSqlParser.TimeStampFunctionContext context) {
			SqlExpression arg;
			if (context.bind_variable() != null) {
				arg = SqlExpression.VariableReference(Name.Variable(context.bind_variable()));
			} else if (context.argString != null) {
				arg = SqlExpression.Constant(InputString.AsNotQuoted(context.argString));
			} else {
				throw new ParseCanceledException("Invalid argument in a TIMESTAMP implicit function");
			}

			SqlExpression tzArg = null;
			if (context.tzString != null) {
				tzArg = SqlExpression.Constant(InputString.AsNotQuoted(context.tzString));
			}

			var args = tzArg != null
				? new SqlExpression[] {arg, tzArg}
				: new SqlExpression[] {arg};

			return SqlExpression.FunctionCall("TOTIMESTAMP", args);
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
			var arg1 = Expression.Build(context.concatenationWrapper());
			var part = "both";
			if (context.LEADING() != null) {
				part = "leading";
			} else if (context.TRAILING() != null) {
				part = "trailing";
			} else if (context.BOTH() != null) {
				part = "both";
			}

			var toTrim = " ";
			if (context.quoted_string() != null) {
				toTrim = InputString.AsNotQuoted(context.quoted_string());
			}

			var arg2 = SqlExpression.Constant(part);
			var arg3 = SqlExpression.Constant(toTrim);

			return SqlExpression.FunctionCall("SQL_TRIM", new SqlExpression[] {arg1, arg2, arg3});
		}

		public override SqlExpression VisitConstantMinValue(PlSqlParser.ConstantMinValueContext context) {
			return SqlExpression.Reference(new ObjectName("MINVALUE"));
		}

		public override SqlExpression VisitConstantMaxValue(PlSqlParser.ConstantMaxValueContext context) {
			return SqlExpression.Reference(new ObjectName("MAXVALUE"));
		}

		public override SqlExpression VisitConstantSessionTimeZone(PlSqlParser.ConstantSessionTimeZoneContext context) {
			return SqlExpression.FunctionCall("CurrentSessionTimeZone");
		}
	}
}
