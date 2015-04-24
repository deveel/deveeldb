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

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// Within an SQL query node, this describes the ordering criteria
	/// that will be applied when returning the results of the selection.
	/// </summary>
	[Serializable]
	public sealed class OrderByNode : SqlNode {
		internal OrderByNode() {
		}

		/// <summary>
		/// Gets the expression used to compare results and put in order.
		/// </summary>
		public IExpressionNode Expression { get; private set; }

		/// <summary>
		/// Gets a value indicating whether the returned ordered set
		/// will be presented in <c>ascending</c> order.
		/// </summary>
		public bool Ascending { get; private set; }

		/// <inheritdoc/>
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