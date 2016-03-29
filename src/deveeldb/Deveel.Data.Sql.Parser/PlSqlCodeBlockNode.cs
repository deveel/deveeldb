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

namespace Deveel.Data.Sql.Parser {
	class PlSqlCodeBlockNode : SqlNode {
		public IEnumerable<IStatementNode> Statements { get; private set; }

		public IEnumerable<ExceptionHandlerNode> ExceptionHandlers { get; private set; } 
 
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("plsql_statement_list")) {
				GetStatements(node);
			} else if (node.NodeName.Equals("exception_block_opt")) {
				ExceptionHandlers = node.FindNodes<ExceptionHandlerNode>();
			}

			return base.OnChildNode(node);
		}

		private void GetStatements(ISqlNode node) {
			var statements = new List<IStatementNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName.Equals("plsql_statement")) {
					var statementNode = childNode.FindNode<IStatementNode>();
					if (statementNode != null)
						statements.Add(statementNode);
				}
			}

			Statements = statements.ToArray();
		}
	}
}
