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
using System.Linq;
using System.Text;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// A node describing the <c>JOIN</c> between two sources within a query.
	/// </summary>
	/// <seealso cref="IFromSourceNode"/>
	class JoinNode : SqlNode {
		internal JoinNode() {	
		}

		public IFromSourceNode Source { get; private set; }

		/// <summary>
		/// Gets the expression used as condition for creating the joined
		/// group in a query.
		/// </summary>
		/// <remarks>
		/// This value can be <c>null</c> only if two sources are joined naturally,
		/// that means the condition is defined in the <c>WHERE</c> expression.
		/// </remarks>
		public IExpressionNode OnExpression { get; private set; }

		/// <summary>
		/// Gets the type of join, as a <see cref="string"/> that will
		/// be operated between the two sources
		/// </summary>
		/// <remarks>
		/// <para>
		/// This value is <c>null</c> if the join is done naturally,
		/// otherwise it can be only one of <c>INNER</c>, <c>OUTER</c>,
		/// <c>LEFT OUTER</c>, <c>RIGHT OUTER</c>.
		/// </para>
		/// </remarks>
		/// <seealso cref="Expressions.JoinType"/>
		public string JoinType { get; private set; }

		public JoinNode NextJoin { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "join_type") {
				GetJoinType(node);
			} else if (node is IFromSourceNode) {
				Source = (IFromSourceNode) node;
			} else if (node.NodeName == "from_source") {
				Source = (IFromSourceNode) node.ChildNodes.FirstOrDefault();
			} else if (node.NodeName == "on_opt") {
				OnExpression = node.FindNode<IExpressionNode>();
			} else if (node.NodeName == "join_opt") {
				var join = node.ChildNodes.FirstOrDefault();
				if (join != null)
					NextJoin = (JoinNode) join;
			}

			return base.OnChildNode(node);
		}

		private void GetJoinType(ISqlNode node) {
			var sb = new StringBuilder();
			bool first = true;
			foreach (var childNode in node.ChildNodes) {
				if (!first)
					sb.Append(" ");

				sb.Append(childNode.NodeName);
				first = false;
			}

			JoinType = sb.ToString();
		}
	}
}