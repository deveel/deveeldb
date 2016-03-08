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

		protected override void BuildStatement(SqlStatementBuilder builder) {
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