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
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class SqlBinaryExpressionNode : SqlNode, IExpressionNode {
		private bool leftSeen;

		internal SqlBinaryExpressionNode() {
		}

		public IExpressionNode Left { get; private set; }

		public IExpressionNode Right { get; private set; }

		public bool IsAll { get; private set; }

		public bool IsAny { get; private set; }

		public string Operator { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				if (!leftSeen) {
					Left = (IExpressionNode) node;
					leftSeen = true;
				} else {
					Right = (IExpressionNode) node;
					leftSeen = false;
				}
			} else if (node.NodeName == "binary_op") {
				GetOperator(node);
			}

			return base.OnChildNode(node);
		}

		private void GetOperator(ISqlNode node) {
			var childNode = node.ChildNodes.First();
			if (childNode.NodeName == "binary_op_simple" ||
				childNode.NodeName == "logical_op") {
				var op = childNode.ChildNodes.First();
				Operator = ((SqlKeyNode) op).Text;
			} else if (node.NodeName == "any_op" ||
			           node.NodeName == "all_op") {
				GetAnyAllOp(childNode);
			} else if (childNode.NodeName == "subquery_op") {
				GetLogicalOp(childNode);
			}
		}

		private void GetLogicalOp(ISqlNode node) {
			var sb = new StringBuilder();
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					sb.Append(((SqlKeyNode) childNode).Text);
					sb.Append(" ");
				}
			}

			Operator = sb.ToString();
		}

		private void GetAnyAllOp(ISqlNode node) {
			var sb = new StringBuilder();
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					var anyOrAll = ((SqlKeyNode) childNode).Text;
					if (String.Equals(anyOrAll, "ALL", StringComparison.OrdinalIgnoreCase)) {
						IsAll = true;
					} else if (String.Equals(anyOrAll, "ANY", StringComparison.OrdinalIgnoreCase)) {
						IsAny = true;
					}
				} else if (childNode.NodeName == "binary_op_simple") {
					var op = childNode.ChildNodes.First();
					sb.Append(((SqlKeyNode) op).Text);
				}
			}

			Operator = sb.ToString();
		}
	}
}