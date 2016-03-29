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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Parser {
	class ExpressionBuilder {
		public static SqlExpression Build(IExpressionNode node) {
			if (node is SqlVariableRefExpressionNode)
				return VisitVariableRefExpression((SqlVariableRefExpressionNode) node);
			if (node is SqlExpressionTupleNode)
				return VisitTupleExpression((SqlExpressionTupleNode) node);
			if (node is SqlQueryExpressionNode)
				return VisitQueryExpression((SqlQueryExpressionNode) node);
			if (node is SqlCaseExpressionNode)
				return VisitCaseExpression((SqlCaseExpressionNode) node);
			if (node is SqlConstantExpressionNode)
				return VisitConstantExpression((SqlConstantExpressionNode) node);
			if (node is SqlFunctionCallExpressionNode)
				return VisitFunctionCall((SqlFunctionCallExpressionNode) node);
			if (node is SqlReferenceExpressionNode)
				return VisitReferenceExpression((SqlReferenceExpressionNode) node);
			if (node is SqlBinaryExpressionNode)
				return VisitBinaryExpression((SqlBinaryExpressionNode) node);
			if (node is SqlUnaryExpressionNode)
				return VisitUnaryExpression((SqlUnaryExpressionNode) node);
			if (node is SqlBetweenExpressionNode)
				return VisitBetweenExpression((SqlBetweenExpressionNode) node);
			if (node is SqlParameterReferenceNode)
				return VisitParameterReferenceExpression((SqlParameterReferenceNode) node);

			if (node is NextSequenceValueNode)
				return VisitNextValueForExpression((NextSequenceValueNode) node);
			if (node is CurrentTimeFunctionNode)
				return VisitCurrentTimeFunctionExpression((CurrentTimeFunctionNode) node);
			if (node is CastExpressionNode)
				return VisitCastExpression((CastExpressionNode) node);

			throw new NotSupportedException();
		}

		private static SqlFunctionCallExpression VisitCastExpression(CastExpressionNode node) {
			// TODO: ExpressionBuilder does not support the ITypeResolver yet, so cast happens only to primitive types
			var expression = Build(node.Expression);
			var dataType = DataTypeBuilder.Build(null, node.DataType);
			var dataTypeArg = SqlExpression.Constant(dataType.ToString());
			return SqlExpression.FunctionCall("cast", new[] {expression, dataTypeArg});
		}

		private static SqlExpression VisitParameterReferenceExpression(SqlParameterReferenceNode node) {
			return new SqlParameterExpression();
		}

		private static SqlExpression VisitCurrentTimeFunctionExpression(CurrentTimeFunctionNode node) {
			return SqlExpression.FunctionCall(node.FunctionName);
		}

		private static SqlExpression VisitNextValueForExpression(NextSequenceValueNode node) {
			return SqlExpression.FunctionCall("NEXT_VALUE", new[] {SqlExpression.Constant(node.SequenceName.Name)});
		}

		private static SqlExpression VisitVariableRefExpression(SqlVariableRefExpressionNode node) {
			return SqlExpression.VariableReference(node.Variable);
		}

		private static SqlExpression VisitTupleExpression(SqlExpressionTupleNode node) {
			return SqlExpression.Tuple(node.Expressions.Select(Build).ToArray());
		}

		private static SqlExpression VisitQueryExpression(SqlQueryExpressionNode node) {
			var selectColumns = GetSelectColumns(node);
			var exp = new SqlQueryExpression(selectColumns) {
				Distinct = node.IsDistinct
			};

			if (node.FromClause != null) {
				SetFromClause(exp.FromClause, node.FromClause);
			}

			if (node.WhereExpression != null) {
				exp.WhereExpression = Build(node.WhereExpression);
			}

			if (node.GroupBy != null) {
				var groupBy = new List<SqlExpression>();
				if (node.GroupBy.GroupExpressions != null)
					groupBy.AddRange(node.GroupBy.GroupExpressions.Select(Build));

				exp.GroupBy = groupBy.ToList();

				var having = node.GroupBy.HavingExpression;
				if (having != null)
					exp.HavingExpression = Build(having);

			}

			if (node.Composite != null) {
				var compositeExp = Build(node.Composite.QueryExpression);
				exp.NextComposite = compositeExp as SqlQueryExpression;
				exp.IsCompositeAll = node.Composite.IsAll;
				exp.CompositeFunction = GetCompositeFunction(node.Composite.CompositeFunction);
			}

			return exp;
		}

		private static CompositeFunction GetCompositeFunction(string s) {
			if (String.Equals(s, "UNION", StringComparison.OrdinalIgnoreCase))
				return CompositeFunction.Union;
			if (String.Equals(s, "EXCEPT", StringComparison.OrdinalIgnoreCase))
				return CompositeFunction.Except;
			if (String.Equals(s, "INTERSECT", StringComparison.OrdinalIgnoreCase))
				return CompositeFunction.Intersect;
			
			throw new InvalidOperationException(String.Format("Composite function {0} is invalid.", s));
		}

		private static void SetFromTableInClause(FromClause clause, IFromSourceNode source, JoinNode join) {
			AddSourceToClause(clause, source);

			if (join != null) {
				var joinType = JoinType.Inner;
				if (!String.IsNullOrEmpty(join.JoinType))
					joinType = GetJoinType(join.JoinType);

				SqlExpression onExpression = null;
				if (join.OnExpression != null)
					onExpression = Build(join.OnExpression);

				clause.Join(joinType, onExpression);

				SetFromTableInClause(clause, join.Source, join.NextJoin);
			}
		}

		private static void AddSourceToClause(FromClause clause, IFromSourceNode source) {
			string alias = null;
			if (source.Alias != null)
				alias = source.Alias.Text;

			if (source is FromTableSourceNode) {
				var tableSource = (FromTableSourceNode)source;
				clause.AddTable(alias, tableSource.TableName.Name);
			} else if (source is FromQuerySourceNode) {
				var querySource = (FromQuerySourceNode)source;
				var queryExpression = (SqlQueryExpression) Build(querySource.Query);
				clause.AddSubQuery(alias, queryExpression);
			}
		}

		private static JoinType GetJoinType(string typeName) {
			if (String.Equals(typeName, "INNER", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "INNER JOIN", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(typeName, ",", StringComparison.OrdinalIgnoreCase))
				return JoinType.Inner;
			if (String.Equals(typeName, "LEFT OUTER", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "LFT OUTER JOIN", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(typeName, "LEFT", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "LFT JOIN", StringComparison.OrdinalIgnoreCase))
				return JoinType.Left;
			if (String.Equals(typeName, "RIGHT OUTER", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "RIGHT OUTER JOIN", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(typeName, "RIGHT", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "RIGHT JOIN", StringComparison.OrdinalIgnoreCase))
				return JoinType.Right;

			return JoinType.None;
		}

		private static void SetFromClause(FromClause clause, FromClauseNode node) {
			SetFromTableInClause(clause, node.Source, node.Join);
		}

		private static IEnumerable<SelectColumn> GetSelectColumns(SqlQueryExpressionNode node) {
			if (node.IsAll) {
				return new[] {new SelectColumn(SqlExpression.Reference(new ObjectName("*")))};
			}

			var items = new List<SelectColumn>();
			foreach (var item in node.SelectItems) {
				SqlExpression exp;
				if (item.Name != null) {
					exp = SqlExpression.Reference(ObjectName.Parse(item.Name.Name));
				} else if (item.Expression != null) {
					exp = Build(item.Expression);
				} else {
					throw new InvalidOperationException();
				}

				string alias = null;
				if (item.Alias != null)
					alias = item.Alias.Text;

				items.Add(new SelectColumn(exp, alias));
			}

			return items.ToArray();
		}

		private static SqlExpression VisitCaseExpression(SqlCaseExpressionNode expressionNode) {
			throw new NotImplementedException();
		}

		private static SqlExpression VisitConstantExpression(SqlConstantExpressionNode expressionNode) {
			var sqlValue = expressionNode.Value;
			Field obj;
			if (sqlValue is SqlString) {
				obj = Field.VarChar((SqlString) sqlValue);
			} else if (sqlValue is SqlBoolean) {
				obj = Field.Boolean((SqlBoolean) sqlValue);
			} else if (sqlValue is SqlNumber) {
				obj = Field.Number((SqlNumber) sqlValue);
			} else if (sqlValue is SqlNull) { 
				obj = Field.Null();
			} else {
				throw new NotSupportedException("Constant value not supported.");
			}

			return SqlExpression.Constant(obj);
		}

		private static SqlExpression VisitReferenceExpression(SqlReferenceExpressionNode node) {
			return SqlExpression.Reference(ObjectName.Parse(node.Reference.Name));
		}

		private static SqlExpression VisitFunctionCall(SqlFunctionCallExpressionNode node) {
			var args = new List<SqlExpression>();
			if (node.Arguments != null)
				args.AddRange(node.Arguments.Select(Build));

			return SqlExpression.FunctionCall(node.FunctionName, args.ToArray());
		}

		private static SqlExpression VisitBetweenExpression(SqlBetweenExpressionNode expressionNode) {
			var testExp = Build(expressionNode.Expression);
			var minValue = Build(expressionNode.MinValue);
			var maxValue = Build(expressionNode.MaxValue);

			var smallerExp = SqlExpression.SmallerOrEqualThan(testExp, maxValue);
			var greaterExp = SqlExpression.GreaterOrEqualThan(testExp, minValue);

			SqlExpression exp = SqlExpression.And(smallerExp, greaterExp);

			if (expressionNode.Not)
				exp = SqlExpression.Not(exp);

			return exp;
		}

		private static SqlExpression VisitBinaryExpression(SqlBinaryExpressionNode expressionNode) {
			var left = Build(expressionNode.Left);
			var right = Build(expressionNode.Right);
			var op = expressionNode.Operator;

			var expType = GetBinaryExpressionType(op);

			// in case of IS NOT simplify
			if (expType == SqlExpressionType.Is &&
			    right is SqlUnaryExpression) {
				var unary = (SqlUnaryExpression) right;
				if (unary.ExpressionType == SqlExpressionType.Not) {
					expType = SqlExpressionType.IsNot;
					right = unary.Operand;
				}
			}

			if (expressionNode.IsAll) {
				expType = MakeAll(expType);
			} else if (expressionNode.IsAny) {
				expType = MakeAny(expType);
			}

			return SqlExpression.Binary(left, expType, right);
		}

		private static SqlExpressionType MakeAll(SqlExpressionType binaryType) {
			if (binaryType == SqlExpressionType.Equal)
				return SqlExpressionType.AllEqual;
			if (binaryType == SqlExpressionType.NotEqual)
				return SqlExpressionType.AllNotEqual;
			if (binaryType == SqlExpressionType.GreaterThan)
				return SqlExpressionType.AllGreaterThan;
			if (binaryType == SqlExpressionType.GreaterOrEqualThan)
				return SqlExpressionType.AllGreaterOrEqualThan;
			if (binaryType == SqlExpressionType.SmallerThan)
				return SqlExpressionType.AllSmallerThan;
			if (binaryType == SqlExpressionType.SmallerOrEqualThan)
				return SqlExpressionType.AllGreaterOrEqualThan;

			throw new NotSupportedException(String.Format("The operator '{0}' cannot be in ALL form", binaryType));
		}

		private static SqlExpressionType MakeAny(SqlExpressionType binaryType) {
			if (binaryType == SqlExpressionType.Equal)
				return SqlExpressionType.AnyEqual;
			if (binaryType == SqlExpressionType.NotEqual)
				return SqlExpressionType.AnyNotEqual;
			if (binaryType == SqlExpressionType.GreaterThan)
				return SqlExpressionType.AnyGreaterThan;
			if (binaryType == SqlExpressionType.GreaterOrEqualThan)
				return SqlExpressionType.AnyGreaterOrEqualThan;
			if (binaryType == SqlExpressionType.SmallerThan)
				return SqlExpressionType.AnySmallerThan;
			if (binaryType == SqlExpressionType.SmallerOrEqualThan)
				return SqlExpressionType.AnyGreaterOrEqualThan;

			throw new NotSupportedException(String.Format("The operator '{0}' cannot be in ANY form", binaryType));
		}

		private static SqlExpressionType GetBinaryExpressionType(string op) {
			if (op == "+" ||
			    op == "||")
				return SqlExpressionType.Add;
			if (op == "-")
				return SqlExpressionType.Subtract;
			if (op == "*")
				return SqlExpressionType.Multiply;
			if (op == "/")
				return SqlExpressionType.Divide;
			if (op == "%" ||
			    String.Equals(op, "MOD", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Modulo;
			if (op == "=")
				return SqlExpressionType.Equal;
			if (op == "<>")
				return SqlExpressionType.NotEqual;
			if (op == ">")
				return SqlExpressionType.GreaterThan;
			if (op == ">=")
				return SqlExpressionType.GreaterOrEqualThan;
			if (op == "<")
				return SqlExpressionType.SmallerThan;
			if (op == "<=")
				return SqlExpressionType.SmallerOrEqualThan;
			if (String.Equals(op, "LIKE", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Like;
			if (String.Equals(op, "NOT LIKE", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.NotLike;

			if (String.Equals(op, "IS", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Is;
			if (String.Equals(op, "IS NOT", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.IsNot;

			if (String.Equals(op, "AND", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.And;
			if (String.Equals(op, "OR", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Or;

			if (String.Equals(op, "IN", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.AnyEqual;
			if (String.Equals(op, "NOT IN", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.AllNotEqual;

			throw new ArgumentException(String.Format("The operator '{0}' is not a binary one.", op));
		}

		private static SqlExpression VisitUnaryExpression(SqlUnaryExpressionNode expressionNode) {
			var expressionType = GetUnaryExpressionType(expressionNode.Operator);
			var operand = Build(expressionNode.Operand);

			return SqlExpression.Unary(expressionType, operand);
		}

		private static SqlExpressionType GetUnaryExpressionType(string op) {
			if (op == "+")
				return SqlExpressionType.UnaryPlus;
			if (op == "-")
				return SqlExpressionType.Negate;
			if (String.Equals(op, "NOT", StringComparison.OrdinalIgnoreCase))
				return SqlExpressionType.Not;

			throw new ArgumentException(String.Format("The operator {0} is not a unary one.", op));
		}
	}
}
