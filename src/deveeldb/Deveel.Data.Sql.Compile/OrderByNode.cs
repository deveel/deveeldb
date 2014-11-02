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
	public sealed class OrderByNode : SqlNode {
		public IExpressionNode Expression { get; private set; }

		public bool Ascending { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "sql_expression") {
				Expression = (IExpressionNode) node;
			} else if (node.NodeName == "sort_order") {
				GetOrder(node);
			}

			return base.OnChildNode(node);
		}

		private void GetOrder(ISqlNode node) {
			if (node is SqlKeyNode) {
				var keyNode = (SqlKeyNode) node;
				if (String.Equals(keyNode.Text, "ASC", StringComparison.OrdinalIgnoreCase)) {
					Ascending = true;
				} else if (String.Equals(keyNode.Text, "DESC", StringComparison.OrdinalIgnoreCase)) {
					Ascending = false;
				}
			}
		}
	}
}