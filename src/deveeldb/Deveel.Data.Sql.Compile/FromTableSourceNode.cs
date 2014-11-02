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

namespace Deveel.Data.Sql.Compile {
	public sealed class FromTableSourceNode : SqlNode, IFromSourceNode {
		public string Alias { get; private set; }

		public ObjectNameNode TableName { get; private set; }

		public JoinNode Join { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = (ObjectNameNode) node;
			} else if (node.NodeName == "select_as_opt") {
				foreach (var childNode in node.ChildNodes) {
					if (childNode is SqlKeyNode &&
						childNode.NodeName == "AS")
						continue;
					if (childNode is IdentifierNode) {
						Alias = ((IdentifierNode) childNode).Text;
					}
				}
			} else if (node is JoinNode) {
				Join = (JoinNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}