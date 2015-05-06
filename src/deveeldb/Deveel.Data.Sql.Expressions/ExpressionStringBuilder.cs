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
using System.Text;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	class ExpressionStringBuilder : SqlExpressionVisitor {
		private readonly StringBuilder builder;
		private bool rootQuery = true;

		public ExpressionStringBuilder() {
			builder = new StringBuilder();
		}

		public string ToSqlString(SqlExpression expression) {
			rootQuery = expression is SqlQueryExpression;

			Visit(expression);
			return builder.ToString();
		}

		public override SqlExpression VisitAssign(SqlAssignExpression assign) {
			Visit(assign.Reference);
			builder.Append(" = ");
			Visit(assign.ValueExpression);

			return assign;
		}

		public override SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
			Visit(binaryEpression.Left);

			var binaryOpString = GetBinaryOperatorString(binaryEpression.ExpressionType);
			builder.AppendFormat(" {0} ", binaryOpString);

			Visit(binaryEpression.Right);

			return binaryEpression;
		}

		private static string GetBinaryOperatorString(SqlExpressionType expressionType) {
			switch (expressionType) {
				case SqlExpressionType.Add:
					return "+";
				case SqlExpressionType.Subtract:
					return "-";
				case SqlExpressionType.Divide:
					return "/";
				case SqlExpressionType.Multiply:
					return "*";
				case SqlExpressionType.Modulo:
					return "%";
				case SqlExpressionType.Equal:
					return "=";
				case SqlExpressionType.NotEqual:
					return "<>";
				case SqlExpressionType.GreaterThan:
					return ">";
				case SqlExpressionType.GreaterOrEqualThan:
					return ">=";
				case SqlExpressionType.SmallerThan:
					return "<";
				case SqlExpressionType.SmallerOrEqualThan:
					return "<=";
				case SqlExpressionType.Is:
					return "IS";
				case SqlExpressionType.IsNot:
					return "IS NOT";
				case SqlExpressionType.Like:
					return "LIKE";
				case SqlExpressionType.NotLike:
					return "NOT LIKE";
				case SqlExpressionType.AllEqual:
					return "= ALL";
				case SqlExpressionType.AllNotEqual:
					return "<> ALL";
				case SqlExpressionType.AllGreaterThan:
					return "> ALL";
				case SqlExpressionType.AllGreaterOrEqualThan:
					return ">= ALL";
				case SqlExpressionType.AllSmallerThan:
					return "< ALL";
				case SqlExpressionType.AllSmallerOrEqualThan:
					return "<= ALL";
				case SqlExpressionType.AnyEqual:
					return "= ANY";
				case SqlExpressionType.AnyNotEqual:
					return "<> ANY";
				case SqlExpressionType.AnyGreaterThan:
					return "> ANY";
				case SqlExpressionType.AnyGreaterOrEqualThan:
					return ">= ANY";
				case SqlExpressionType.AnySmallerThan:
					return "< ANY";
				case SqlExpressionType.AnySmallerOrEqualThan:
					return "<= ANY";
				case SqlExpressionType.Or:
					return "OR";
				case SqlExpressionType.And:
					return "AND";
				case SqlExpressionType.XOr:
					return "XOR";
				default:
					throw new NotSupportedException();
			}
		}

		public override SqlExpression VisitCast(SqlCastExpression castExpression) {
			builder.Append("CAST ");
			Visit(castExpression.Value);
			builder.Append(" AS ");
			builder.Append(castExpression.DataType);

			return base.VisitCast(castExpression);
		}

		public override SqlExpression VisitConditional(SqlConditionalExpression conditional) {
			return base.VisitConditional(conditional);
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (value.Type is QueryType) {
				// TODO: convert to sql string also a QUERY PLAN
				builder.Append("({QUERY})");
			} else if (value.Type is ArrayType) {
				var array = (SqlArray) value.Value;
				if (array.IsNull) {
					builder.Append("NULL");
				} else {
					builder.Append("(");
					var sz = array.Length;
					for (int i = 0; i < sz; i++) {
						Visit(array[i]);

						if (i < sz - 1)
							builder.Append(", ");
					}
					builder.Append(")");
				}
			} else if (value.Type is NullType) {
				builder.Append("NULL");
			} else if (value.Type.IsPrimitive) {
				if (value.IsNull) {
					builder.Append("NULL");
				} else {
					builder.Append(value.Value);
				}
			}

			return constant;
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			builder.Append(expression.FunctioName);
			builder.Append("(");

			if (expression.Arguments != null &&
			    expression.Arguments.Length > 0) {
				var args = expression.Arguments;
				var argc = args.Length;

				for (int i = 0; i < argc; i++) {
					Visit(args[i]);

					if (i < argc - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");

			return expression;
		}

		private void PrintQueryColumns(IEnumerable<SelectColumn> selectColumns) {
			var columns = selectColumns.ToArray();
			var sz = columns.Length;
			for (int i = 0; i < sz; i++) {
				var column = columns[i];

				if (column.IsGlob) {
					if (column.IsAll) {
						builder.Append("*");
					} else {
						builder.AppendFormat("{0}.*", column.TableName);
					}
				} else {
					Visit(column.Expression);
				}

				if (!String.IsNullOrEmpty(column.Alias))
					builder.AppendFormat(" AS {0}", column.Alias);

				if (i < sz - 1)
					builder.Append(", ");
			}
		}

		public override SqlExpression VisitQuery(SqlQueryExpression query) {
			if (!rootQuery)
				builder.Append("(");

			builder.Append("SELECT ");
			if (query.Distinct)
				builder.Append("DISTINCT ");

			PrintQueryColumns(query.SelectColumns);
			builder.Append(" ");

			PrintFromClause(query.FromClause);

			if (query.WhereExpression != null) {
				builder.Append(" WHERE ");
				Visit(query.WhereExpression);
			}

			if (query.GroupBy != null) {
				builder.Append(" GROUP BY ");
				VisitExpressionList(query.GroupBy.ToArray());

				if (query.HavingExpression != null) {
					builder.Append(" HVAING ");
					Visit(query.HavingExpression);
				}
			}

			if (query.GroupMax != null) {
				builder.Append(" GROUP MAX ");
				builder.Append(query.GroupMax.FullName);
			}

			// TODO: COMPOSITE ...

			if (!rootQuery)
				builder.Append(")");

			return query;
		}

		private void PrintFromClause(FromClause fromClause) {
			builder.Append("FROM ");

			var tables = fromClause.AllTables.ToList();
			for (int i = 0; i < tables.Count; i++) {
				var source = tables[i];

				JoinPart joinPart = null;

				if (i > 0) {
					joinPart = fromClause.GetJoinPart(i - 1);
					if (joinPart != null) {
						if (joinPart.JoinType == JoinType.Inner) {
							builder.Append(" INNER JOIN ");
						} else if (joinPart.JoinType == JoinType.Right) {
							builder.Append(" RIGHT OUTER JOIN ");
						} else if (joinPart.JoinType == JoinType.Left) {
							builder.Append(" LEFT OUTER JOIN ");
						} else if (joinPart.JoinType == JoinType.Full) {
							builder.Append(" FULL OUTER JOINT ");
						}
					}
				}

				if (source.IsSubQuery) {
					builder.Append("(");
					Visit(source.SubQuery);
					builder.Append(")");
				} else {
					builder.Append(source.Name);
				}

				if (!String.IsNullOrEmpty(source.Alias)) {
					builder.Append(" AS ");
					builder.Append(source.Alias);
				}

				if (i < tables.Count - 1) {
					if (joinPart == null) {
						builder.Append(", ");
					} else {
						builder.Append(" ON ");
						Visit(joinPart.OnExpression);
					}
				}
			}
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			builder.Append(reference.ReferenceName);
			return reference;
		}

		public override SqlExpression VisitTuple(SqlTupleExpression expression) {
			builder.Append("(");

			var sz = expression.Expressions.Length;
			for (int i = 0; i < sz; i++) {
				Visit(expression.Expressions[i]);
				if (i < sz - 1)
					builder.Append(", ");
			}

			builder.Append(")");
			return expression;
		}

		public override SqlExpression VisitUnary(SqlUnaryExpression unary) {
			var unaryOpString = GetUnaryOperatorString(unary.ExpressionType);
			builder.Append(unaryOpString);
			builder.Append(" ");
			Visit(unary.Operand);

			return unary;
		}

		private string GetUnaryOperatorString(SqlExpressionType unaryType) {
			switch (unaryType) {
				case SqlExpressionType.UnaryPlus:
					return "+";
				case SqlExpressionType.Negate:
					return "-";
				case SqlExpressionType.Not:
					return "NOT";
				default:
					throw new NotSupportedException();
			}
		}

		public override SqlExpression VisitVariableReference(SqlVariableReferenceExpression reference) {
			builder.AppendFormat(":{0}", reference.VariableName);
			return reference;
		}
	}
}
