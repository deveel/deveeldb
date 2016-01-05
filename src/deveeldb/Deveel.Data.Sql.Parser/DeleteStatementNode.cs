using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DeleteStatementNode : SqlStatementNode {
		public string TableName { get; private set; }

		public IExpressionNode WhereExpression { get; private set; }

		public long Limit { get; private set; }

		public bool FromCursor { get; private set; }

		public string CursorName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName.Equals("where_opt")) {
				var childNode = node.ChildNodes.FirstOrDefault();
				if (childNode != null) {
					if (childNode.NodeName.Equals("where")) {
						GetWhere(childNode);
					} else if (childNode.NodeName.Equals("current_of")) {
						GetFromCursor(childNode);
					}
				}
			}

			return base.OnChildNode(node);
		}

		private void GetWhere(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode) {
					WhereExpression = (IExpressionNode) childNode;
				} else if (childNode.NodeName.Equals("limit_opt")) {
					GetLimit(childNode);
				}
			}
		}

		private void GetLimit(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IntegerLiteralNode) {
					Limit = ((IntegerLiteralNode) childNode).Value;
				}
			}
		}

		private void GetFromCursor(ISqlNode node) {
			var idNode = node.FindNode<IdentifierNode>();
			if (idNode != null) {
				FromCursor = true;
				CursorName = idNode.Text;
			}
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var tableName = ObjectName.Parse(TableName);

			if (!FromCursor) {
				var whereExp = ExpressionBuilder.Build(WhereExpression);
				builder.AddObject(new DeleteStatement(tableName, whereExp, Limit));
			} else {
				builder.AddObject(new DeleteCurrentStatement(tableName, CursorName));
			}
		}
	}
}