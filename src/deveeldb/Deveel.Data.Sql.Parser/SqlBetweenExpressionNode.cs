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
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// An SQL <c>BETWEEN</c> expression node that evaluates to <c>true</c>
	/// if the <see cref="Expression"/> given is between <see cref="MinValue"/>
	/// (inclusive) and <see cref="MaxValue"/> (exclusive).
	/// </summary>
	[Serializable]
	class SqlBetweenExpressionNode : SqlNode, IExpressionNode {
		private bool expressionSeen;
		private bool minValueSeen;

		/// <summary>
		/// Gets the expression to be tested against <see cref="MinValue"/>
		/// and <see cref="MaxValue"/>.
		/// </summary>
		public IExpressionNode Expression { get; private set; }

		/// <summary>
		/// Gets the minimum value (inclusive) that <see cref="Expression"/>
		/// must be to evaluate to <c>true</c>.
		/// </summary>
		public IExpressionNode MinValue { get; private set; }

		/// <summary>
		/// Gets the maximum value (inclusive) that <see cref="Expression"/>
		/// must be to evaluate to <c>true</c>.
		/// </summary>
		public IExpressionNode MaxValue { get; private set; }

		public bool Not { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				if (!expressionSeen) {
					Expression = (IExpressionNode) node;
					expressionSeen = true;
				} else if (!minValueSeen) {
					MinValue = (IExpressionNode) node;
					minValueSeen = true;
				} else {
					MaxValue = (IExpressionNode) node;
				}
			} else if (node.NodeName == "not_opt") {
				Not = node.ChildNodes.Any();
			}

			return base.OnChildNode(node);
		}
	}
}