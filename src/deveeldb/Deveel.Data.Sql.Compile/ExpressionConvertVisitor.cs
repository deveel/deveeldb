// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Compile {
	public class ExpressionConvertVisitor : SqlNodeVisitor {
		public ExpressionConvertVisitor(IExpressionNode inputNode) {
			if (inputNode == null) 
				throw new ArgumentNullException("inputNode");

			InputNode = inputNode;
		}

		public IExpressionNode InputNode { get; private set; }

		public SqlExpression OutputExpression { get; private set; }

		public SqlExpression Convert() {
			VisitNode(InputNode);
			return OutputExpression;
		}

		protected override void VisitNode(ISqlNode node) {
			if (node is IExpressionNode)
				OutputExpression = VisitExpression((IExpressionNode) node);

			base.VisitNode(node);
		}

		private SqlExpression VisitExpression(IExpressionNode expressionNode) {
			if (expressionNode is SqlUnaryExpressionNode)
				return VisitUnaryExpression((SqlUnaryExpressionNode) expressionNode);
			if (expressionNode is SqlBinaryExpressionNode)
				return VisitBinaryExpression((SqlBinaryExpressionNode) expressionNode);
			if (expressionNode is SqlBetweenExpressionNode)
				return VisitBetweenExpression((SqlBetweenExpressionNode) expressionNode);
			if (expressionNode is SqlFunctionCallExpressionNode)
				return VisitFunctionCallExpression((SqlFunctionCallExpressionNode) expressionNode);
			if (expressionNode is SqlConstantExpressionNode)
				return VisitConstantExpression((SqlConstantExpressionNode) expressionNode);
			if (expressionNode is SqlCaseExpressionNode)
				return VisitCaseExpression((SqlCaseExpressionNode) expressionNode);
			if (expressionNode is SqlReferenceExpressionNode)
				return VisitReference((SqlReferenceExpressionNode) expressionNode);

			if (expressionNode is SqlQueryExpressionNode)
				return VisitQueryExpression((SqlQueryExpressionNode) expressionNode);

			throw new NotSupportedException();
		}

		private SqlQueryExpression VisitQueryExpression(SqlQueryExpressionNode node) {
			var selectColumns = GetSelectColumns(node);
			var exp = new SqlQueryExpression(selectColumns);

			FromClause fromClause = null;
			if (node.FromClause != null) {
				SetFromClause(exp.FromClause, node.FromClause);
			}

			return exp;
		}

		private void SetFromTableInClause(FromClause clause, IFromSourceNode source) {
			if (source is FromTableSourceNode) {
				var tableSource = (FromTableSourceNode) source;
				clause.AddTable(tableSource.Alias, tableSource.TableName.Name.FullName);
			} else if (source is FromQuerySourceNode) {
				var querySource = (FromQuerySourceNode) source;
				var queryExpression = VisitQueryExpression(querySource.Query);
				clause.AddSubQuery(source.Alias, queryExpression);
			}

			if (source.Join != null) {
				var joinType = JoinType.Inner;
				if (!String.IsNullOrEmpty(source.Join.JoinType))
					joinType = GetJoinType(source.Join.JoinType);

				SqlExpression onExpression = null;
				if (source.Join.OnExpression != null)
					onExpression = VisitExpression(source.Join.OnExpression);

				clause.Join(joinType, onExpression);

				var otherSource = source.Join.OtherSource;
				if (otherSource == null)
					throw new SqlExpressionException("The JOIN clause is not valid: missing other part.");

				SetFromTableInClause(clause, otherSource);
			}
		}

		private JoinType GetJoinType(string typeName) {
			if (String.Equals(typeName, "INNER", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, ",", StringComparison.InvariantCulture))
				return JoinType.Inner;
			if (String.Equals(typeName, "LEFT OUTER", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeName, "LEFT"))
				return JoinType.Left;
			if (String.Equals(typeName, "RIGHT OUTER", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(typeName, "RIGHT", StringComparison.OrdinalIgnoreCase))
				return JoinType.Right;

			return JoinType.None;
		}

		private void SetFromClause(FromClause clause, FromClauseNode node) {
			foreach (var source in node.Sources) {
				SetFromTableInClause(clause, source);
			}
		}

		private IEnumerable<SelectColumn> GetSelectColumns(SqlQueryExpressionNode node) {
			if (node.IsAll || node.SelectAll) {
				return new[] {new SelectColumn(SqlExpression.Reference(new ObjectName("*")))};
			}

			var items = new List<SelectColumn>();
			foreach (var item in node.SelectItems) {
				SqlExpression exp;
				if (item.Name != null) {
					exp = SqlExpression.Reference(item.Name.Name);
				} else if (item.Expression != null) {
					exp = VisitExpression(item.Expression);
				} else {
					throw new InvalidOperationException();
				}

				items.Add(new SelectColumn(exp, item.Alias));
			}

			return items.AsReadOnly();
		} 

		private SqlConditionalExpression VisitCaseExpression(SqlCaseExpressionNode expressionNode) {
			throw new NotImplementedException();
		}

		private SqlConstantExpression VisitConstantExpression(SqlConstantExpressionNode expressionNode) {
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

			return SqlExpression.Constant(obj);
		}

		private SqlReferenceExpression VisitReference(SqlReferenceExpressionNode node) {
			// TODO: support the Variable reference (:var)
			return SqlExpression.Reference(node.Reference.Name);
		}

		private SqlFunctionCallExpression VisitFunctionCallExpression(SqlFunctionCallExpressionNode expressionNode) {
			var args = new List<SqlExpression>();
			if (expressionNode.Arguments != null)
				args.AddRange(expressionNode.Arguments.Select(VisitExpression));

			return SqlExpression.FunctionCall(expressionNode.FunctionName, args.ToArray());
		}

		private SqlBinaryExpression VisitBetweenExpression(SqlBetweenExpressionNode expressionNode) {
			var testExp = VisitExpression(expressionNode.Expression);
			var minValue = VisitExpression(expressionNode.MinValue);
			var maxValue = VisitExpression(expressionNode.MaxValue);

			var smallerExp = SqlExpression.SmallerOrEqualThan(testExp, maxValue);
			var greaterExp = SqlExpression.GreaterOrEqualThan(testExp, minValue);
			return SqlExpression.And(smallerExp, greaterExp);
		}

		private SqlBinaryExpression VisitBinaryExpression(SqlBinaryExpressionNode expressionNode) {
			var left = VisitExpression(expressionNode.Left);
			var right = VisitExpression(expressionNode.Right);
			var op = expressionNode.Operator;

			var expType = GetBinaryExpressionType(op);

			return SqlExpression.Binary(left, expType, right);
		}

		private SqlExpressionType GetBinaryExpressionType(string op) {
			// TODO: support ALL and ANY

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

			throw new ArgumentException(String.Format("The operator {0} is not a binary one."));
		}

		private SqlUnaryExpression VisitUnaryExpression(SqlUnaryExpressionNode expressionNode) {
			throw new NotImplementedException();
		}
	}
}