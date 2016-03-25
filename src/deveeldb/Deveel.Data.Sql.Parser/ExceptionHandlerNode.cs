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

namespace Deveel.Data.Sql.Parser {
	class ExceptionHandlerNode : SqlNode {
		public bool HandlesOthers { get; private set; }

		public IEnumerable<string> HandledExceptionNames { get; private set; }

		public IEnumerable<IStatementNode> Statements { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("handled_exceptions")) {
				GetHandledExceptions(node);
			} else if (node.NodeName.Equals("plsql_statement_list")) {
				GetStatements(node);
			}

			return base.OnChildNode(node);
		}

		private void GetStatements(ISqlNode node) {
			var statements = new List<IStatementNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName.Equals("plsql_statement")) {
					statements.AddRange(childNode.ChildNodes.OfType<IStatementNode>());
				}
			}

			Statements = statements.ToArray();
		}

		private void GetHandledExceptions(ISqlNode node) {
			var exceptionNames = new List<string>();

			foreach (var childNode in node.ChildNodes) {
				if (HandlesOthers)
					throw new SqlParseException("Multiple exception names found when OTHERS clause was specified.", node.Line, node.Column);

				if (childNode is SqlKeyNode &&
				    ((SqlKeyNode) childNode).Text.Equals("OTHERS", StringComparison.OrdinalIgnoreCase)) {
					HandlesOthers = true;
				} else if (childNode is IdentifierNode) {
					exceptionNames.Add(((IdentifierNode)childNode).Text);
				}
			}

			if (!HandlesOthers)
				HandledExceptionNames = exceptionNames.ToArray();
		}
	}
}
