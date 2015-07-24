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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Parser {
	class ExpressionBuilder : SqlNodeVisitor {
		private SqlExpression outputExpression;

		public SqlExpression Build(ISqlNode node) {
			Visit(node);
			return outputExpression;
		}

		public override void VisitVariableRefExpression(SqlVariableRefExpressionNode node) {
			outputExpression = SqlExpression.VariableReference(node.Variable);
		}

		public override void VisitTupleExpression(SqlExpressionTupleNode node) {
			outputExpression = SqlExpression.Tuple(node.Expressions.Select(Build).ToArray());
		}

		public override void VisitQueryExpression(SqlQueryExpressionNode node) {
			var selectColumns = GetSelectColumns(node);
			var exp = new SqlQueryExpression(selectColumns);

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

			outputExpression = exp;
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

		private void SetFromTableInClause(FromClause clause, IFromSourceNode source, JoinNode join) {
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

		private void AddSourceToClause(FromClause clause, IFromSourceNode source) {
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

		private JoinType GetJoinType(string typeName) {
			if (String.Equals(typeName, "INNER", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "INNER JOIN", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(typeName, ",", StringComparison.InvariantCulture))
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

		private void SetFromClause(FromClause clause, FromClauseNode node) {
			SetFromTableInClause(clause, node.Source, node.Join);
		}

		private IEnumerable<SelectColumn> GetSelectColumns(SqlQueryExpressionNode node) {
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

			return items.AsReadOnly();
		}

		public override void VisitCaseExpression(SqlCaseExpressionNode expressionNode) {
			throw new NotImplementedException();
		}

		public override void VisitConstantExpression(SqlConstantExpressionNode expressionNode) {
			var sqlValue = expressionNode.Value;
			DataObject obj;
			if (sqlValue is SqlString) {
				obj = DataObject.VarChar((SqlString) sqlValue);
			} else if (sqlValue is SqlBoolean) {
				obj = DataObject.Boolean((SqlBoolean) sqlValue);
			} else if (sqlValue is SqlNumber) {
				obj = DataObject.Number((SqlNumber) sqlValue);
			} else {
				throw new NotSupportedException("Constant value is not supported.");
			}

			outputExpression = SqlExpression.Constant(obj);
		}

		public override void VisitReferenceExpression(SqlReferenceExpressionNode node) {
			outputExpression = SqlExpression.Reference(ObjectName.Parse(node.Reference.Name));
		}

		public override void VisitFunctionCall(SqlFunctionCallExpressionNode node) {
			var args = new List<SqlExpression>();
			if (node.Arguments != null)
				args.AddRange(node.Arguments.Select(Build));

			outputExpression = SqlExpression.FunctionCall(node.FunctionName, args.ToArray());
		}

		public override void VisitBetweenExpression(SqlBetweenExpressionNode expressionNode) {
			var testExp = Build(expressionNode.Expression);
			var minValue = Build(expressionNode.MinValue);
			var maxValue = Build(expressionNode.MaxValue);

			var smallerExp = SqlExpression.SmallerOrEqualThan(testExp, maxValue);
			var greaterExp = SqlExpression.GreaterOrEqualThan(testExp, minValue);

			outputExpression = SqlExpression.And(smallerExp, greaterExp);

			if (expressionNode.Not)
				outputExpression = SqlExpression.Not(outputExpression);
		}

		public override void VisitBinaryExpression(SqlBinaryExpressionNode expressionNode) {
			var left = Build(expressionNode.Left);
			var right = Build(expressionNode.Right);
			var op = expressionNode.Operator;

			var expType = GetBinaryExpressionType(op);

			outputExpression = SqlExpression.Binary(left, expType, right);
		}

		private SqlExpressionType GetBinaryExpressionType(string op) {
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

			throw new ArgumentException(String.Format("The operator {0} is not a binary one.", op));
		}

		public override void VisitUnaryExpression(SqlUnaryExpressionNode expressionNode) {
			var expressionType = GetUnaryExpressionType(expressionNode.Operator);
			var operand = Build(expressionNode.Operand);

			outputExpression = SqlExpression.Unary(expressionType, operand);
		}

		private SqlExpressionType GetUnaryExpressionType(string op) {
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
