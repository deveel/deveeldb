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

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class SqlUnaryExpressionNode : SqlNode, IExpressionNode {
		public string Operator { get; private set; }

		public IExpressionNode Operand { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				Operand = (IExpressionNode) node;
			} else if (node.NodeName == "unary_op") {
				var op = node.ChildNodes.First();
				if (op is SqlKeyNode)
					Operator = ((SqlKeyNode) op).Text;
			}

			return base.OnChildNode(node);
		}
	}
}