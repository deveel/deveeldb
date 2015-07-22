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

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// Represents the node that is a database table as source of a query.
	/// </summary>
	/// <seealso cref="IFromSourceNode"/>
	class FromTableSourceNode : SqlNode, IFromSourceNode {
		internal FromTableSourceNode() {
		}

		/// <inheritdoc/>
		public IdentifierNode Alias { get; private set; }

		/// <summary>
		/// Gets the name of the table that is set as source of a query.
		/// </summary>
		public ObjectNameNode TableName { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = (ObjectNameNode)node;
			} else if (node.NodeName == "select_as_opt") {
				Alias = (IdentifierNode)node.ChildNodes.FirstOrDefault();
			}

			return base.OnChildNode(node);
		}
	}
}