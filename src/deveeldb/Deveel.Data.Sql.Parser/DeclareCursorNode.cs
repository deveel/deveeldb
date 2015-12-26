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

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DeclareCursorNode : SqlStatementNode, IDeclareNode {
		public string CursorName { get; private set; }

		public IExpressionNode QueryExpression { get; private set; }

		public IEnumerable<CursorParameterNode> Parameters { get; private set; }

		public bool Insensitive { get; private set; }

		public bool Scroll { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				CursorName = ((IdentifierNode) node).Text;
			} else if (node.NodeName.Equals("cursor_args_opt")) {
				Parameters = node.FindNodes<CursorParameterNode>();
			} else if (node is IExpressionNode) {
				QueryExpression = (SqlQueryExpressionNode) node;
			} else if (node.NodeName.Equals("insensitive_opt")) {
				Insensitive = node.ChildNodes.Any();
			} else if (node.NodeName.Equals("scroll_opt")) {
				Scroll = node.ChildNodes.Any();
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var parameters = new List<CursorParameter>();
			if (Parameters != null) {
				foreach (var parameterNode in Parameters) {
					var dataType = builder.BuildDataType(parameterNode.ParameterType);
					parameters.Add(new CursorParameter(parameterNode.ParameterName, dataType));
				}
			}

			var flags = new CursorFlags();
			if (Insensitive)
				flags |= CursorFlags.Insensitive;
			if (Scroll)
				flags |= CursorFlags.Scroll;

			var queryExpression = (SqlQueryExpression) ExpressionBuilder.Build(QueryExpression);

			builder.Objects.Add(new DeclareCursorStatement(CursorName, parameters.ToArray(), flags, queryExpression));
		}
	}
}
