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
	class InsertStatementNode : SqlNode, IStatementNode {
		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public ValuesInsertNode ValuesInsert { get; private set; }

		public SetInsertNode SetInsert { get; private set; }

		public QueryInsertNode QueryInsert { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName.Equals("insert_source")) {
				var colNode = node.FindByName("column_list_opt");
				if (colNode != null)
					ColumnNames = colNode.FindNodes<IdentifierNode>().Select(x => x.Text);

				ValuesInsert = node.FindNode<ValuesInsertNode>();
				SetInsert = node.FindNode<SetInsertNode>();
				QueryInsert = node.FindNode<QueryInsertNode>();
			}

			return base.OnChildNode(node);
		}
	}
}
