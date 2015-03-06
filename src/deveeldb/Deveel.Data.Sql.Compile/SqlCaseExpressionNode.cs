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

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// An SQL node describing an in-line <c>CASE</c> conditional expression.
	/// </summary>
	[Serializable]
	public sealed class SqlCaseExpressionNode : SqlNode, IExpressionNode {
		/// <summary>
		/// Gets a read-only list of <see cref="CaseSwitchNode">switches</see>
		/// that will be evaluated to return the result of the expresion.
		/// </summary>
		public IEnumerable<CaseSwitchNode> CaseSwitches { get; private set; }

		/// <summary>
		/// Gets a fallback expression that will be evaluated if none
		/// of <see cref="CaseSwitches"/> is be evaluated.
		/// </summary>
		public IExpressionNode ElseExpression { get; private set; }

		/// <summary>
		/// Gets the main expression to be evaluated against the <see cref="CaseSwitches"/>
		/// </summary>
		public IExpressionNode TestExpression { get; private set; }

		/// <inheritdoc/>
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