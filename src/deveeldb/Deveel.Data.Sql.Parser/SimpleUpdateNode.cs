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

namespace Deveel.Data.Sql.Parser {
	class SimpleUpdateNode : SqlNode {
		internal SimpleUpdateNode() {
			Limit = -1;
		}

		public string TableName { get; private set; }

		public IExpressionNode WhereExpression { get; private set; }

		public IEnumerable<UpdateColumnNode> Columns { get; private set; }

		public int Limit { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName == "column_set_list") {
				Columns = node.FindNodes<UpdateColumnNode>();
			} else if (node.NodeName == "update_where") {
				WhereExpression = node.FindNode<IExpressionNode>();
			} else if (node.NodeName == "limit_opt") {
				var child = node.FindNode<IntegerLiteralNode>();
				if (child != null)
					Limit = (int) child.Value;
			}

			return base.OnChildNode(node);
		}
	}
}
