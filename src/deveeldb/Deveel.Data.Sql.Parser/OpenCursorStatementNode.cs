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
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class OpenCursorStatementNode : SqlStatementNode {
		public string CursorName { get; private set; }

		public IEnumerable<IExpressionNode> Arguments { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				CursorName = ((IdentifierNode) node).Text;
			} else if (node.NodeName.Equals("args_opt")) {
				GetArguments(node);
			}

			return base.OnChildNode(node);
		}

		private void GetArguments(ISqlNode node) {
			var listNode = node.FindByName("arg_list");
			if (listNode == null)
				return;

			var args = new List<IExpressionNode>();

			foreach (var childNode in listNode.ChildNodes) {
				if (childNode is IExpressionNode)
					args.Add((IExpressionNode)childNode);
			}

			Arguments = args.AsEnumerable();
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			var args = new List<SqlExpression>();
			if (Arguments != null) {
				args = Arguments.Select(ExpressionBuilder.Build).ToList();
			}

			builder.AddObject(new OpenStatement(CursorName, args.ToArray()));
		}
	}
}
