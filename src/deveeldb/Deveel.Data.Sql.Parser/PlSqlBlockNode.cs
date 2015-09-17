﻿// 
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

namespace Deveel.Data.Sql.Parser {
	class PlSqlBlockNode : SqlNode {
		public string Label { get; private set; }

		public IEnumerable<IDeclareNode> Declarations { get; private set; }

		public PlSqlCodeBlockNode CodeBlock { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("plsql_label_opt")) {
				Label = node.FindNode<LabelNode>().Text;
			} else if (node.NodeName.Equals("declare_statement_opt")) {
				Declarations = node.FindNodes<IDeclareNode>();
			} else if (node is PlSqlCodeBlockNode) {
				CodeBlock = (PlSqlCodeBlockNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}