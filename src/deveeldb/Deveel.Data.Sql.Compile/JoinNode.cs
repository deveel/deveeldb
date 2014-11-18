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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class JoinNode : SqlNode {
		public SqlBinaryExpressionNode OnExpression { get; private set; }

		public string JoinType { get; private set; }

		public IFromSourceNode OtherSource { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "join_type") {
				GetJoinType(node);
			} else if (node is IFromSourceNode) {
				OtherSource = (IFromSourceNode) node;
			} else if (node.NodeName == "from_source_list") {
				GetTablelist(node);
			} else if (node is SqlBinaryExpressionNode) {
				OnExpression = (SqlBinaryExpressionNode) node;
			}

			return base.OnChildNode(node);
		}

		private void GetTablelist(ISqlNode node) {
			//var list = new List<IFromSourceNode>();
			//foreach (var child in node.ChildNodes) {
			//	var source = child.ChildNodes.First();
			//	if (source is IFromSourceNode)
			//		list.Add((IFromSourceNode)source);
			//}

			//Sources = list.AsReadOnly();
		}

		private void GetJoinType(ISqlNode node) {
			var sb = new StringBuilder();
			foreach (var childNode in node.ChildNodes) {
				sb.Append(childNode.NodeName);
			}
			JoinType = sb.ToString();
		}
	}
}