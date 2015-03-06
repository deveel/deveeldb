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

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class StatementSequenceNode : SqlNode {
		private readonly ICollection<IStatementNode> statementNodes;

		public StatementSequenceNode() {
			statementNodes = new List<IStatementNode>();
		}

		public IEnumerable<IStatementNode> Statements {
			get { return statementNodes.AsEnumerable(); }
		}

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "command") {
				ReadStatements(node.ChildNodes);
			}

			return base.OnChildNode(node);
		}

		private void ReadStatements(IEnumerable<ISqlNode> nodes) {
			foreach (var node in nodes) {
				if (node.NodeName == "sql_statement") {
					var child = node.ChildNodes.FirstOrDefault();
					if (child != null) {
						var statementNode = child.ChildNodes.FirstOrDefault() as IStatementNode;
						if (statementNode != null)
							statementNodes.Add(statementNode);
					}
				} else if (node.NodeName == "sql_command_end_opt") {
					
				}
			}
		}
	}
}