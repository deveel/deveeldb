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

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class FetchStatementNode : SqlStatementNode {
		public string Direction { get; private set; }

		public string CursorName { get; private set; }

		public IExpressionNode Position { get; private set; }

		public IExpressionNode Into { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "direction_opt") {
				GetDirection(node);
			} else if (node is IdentifierNode) {
				CursorName = ((IdentifierNode) node).Text;
			} else if (node.NodeName == "into_opt") {
				GetInto(node);
			}

			return base.OnChildNode(node);
		}

		private void GetDirection(ISqlNode node) {
			var childNode = node.ChildNodes.FirstOrDefault();
			if (childNode == null)
				return;

			childNode = childNode.ChildNodes.FirstOrDefault();
			if (childNode == null)
				throw new SqlParseException();

			if (String.Equals(childNode.NodeName, "NEXT", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(childNode.NodeName, "PRIOR", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(childNode.NodeName, "FIRST", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(childNode.NodeName, "LAST", StringComparison.OrdinalIgnoreCase)) {
				Direction = childNode.NodeName.ToUpper();
			} else if (String.Equals(childNode.NodeName, "ABSOLUTE", StringComparison.OrdinalIgnoreCase) ||
			           String.Equals(childNode.NodeName, "RELATIVE", StringComparison.OrdinalIgnoreCase)) {
				var positionNode = childNode.ChildNodes.FirstOrDefault();
				if (positionNode == null)
					throw new SqlParseException("The position expression if required in an ABSOLUTE or RELATIVE fetch.");

				var expression = positionNode as IExpressionNode;
				if (expression == null)
					throw new SqlParseException();

				Direction = childNode.NodeName.ToUpper();
				Position = expression;
			}
		}

		private void GetInto(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode) {
					Into = childNode as IExpressionNode;
					break;
				}
			}
		}

		private static bool TryParseDirection(string s, out FetchDirection direction) {
#if PCL
			return Enum.TryParse(s, true, out direction);
#else
			try {
				direction = (FetchDirection) Enum.Parse(typeof (FetchDirection), s, true);
				return true;
			} catch (Exception) {
				direction = new FetchDirection();
				return false;
			}
#endif
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			FetchDirection direction;
			if (!TryParseDirection(Direction, out direction))
				throw new InvalidOperationException();

			var statement = new FetchStatement(CursorName, direction);
			if (Into != null)
				statement.IntoReference = ExpressionBuilder.Build(Into);
			if (Position != null)
				statement.PositionExpression = ExpressionBuilder.Build(Position);

			builder.AddObject(statement);
		}
	}
}
