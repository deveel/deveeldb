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

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class SqlCaseExpressionNode : SqlNode, IExpressionNode {
		public IEnumerable<CaseSwitchNode> CaseSwitches { get; private set; }

		public IExpressionNode ElseExpression { get; private set; }

		public IExpressionNode TestExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "case_test_expression_opt") {
				if (node.ChildNodes.Any()) {
					var exp = node.ChildNodes.First();
					if (exp is IExpressionNode)
						TestExpression = (IExpressionNode) exp;
				}
			} else if (node.NodeName == "case_when_then_list") {
				GetWhenThenList(node);
			} else if (node.NodeName == "case_else_opt") {
				GetElse(node);
			}

			return base.OnChildNode(node);
		}

		private void GetElse(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode) {
					ElseExpression = (IExpressionNode) childNode;
					break;
				}
			}
		}

		private void GetWhenThenList(ISqlNode node) {
			var switches = new List<CaseSwitchNode>();

			foreach (var childNode in node.ChildNodes) {
				if (childNode is CaseSwitchNode)
					switches.Add((CaseSwitchNode)childNode);
			}

			CaseSwitches = switches.AsReadOnly();
		}
	}
}