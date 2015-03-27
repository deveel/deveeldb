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

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// A node that descrbes the <c>GROUP BY</c> clause in a SQL query.
	/// </summary>
	[Serializable]
	class GroupByNode : SqlNode {
		/// <summary>
		/// Gets the expression node to group the results.
		/// </summary>
		public IEnumerable<IExpressionNode> GroupExpressions { get; private set; }

		/// <summary>
		/// Gets the <c>HVAING</c> expression used to filter the groupped results.
		/// </summary>
		public IEnumerable<IExpressionNode> HavingExpressions { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "sql_expression_ilist") {
				GetGroupExpressions(node);
			} else if (node.NodeName == "having_clause_opt") {
				GetHavingClause(node);
			}

			return base.OnChildNode(node);
		}

		private void GetHavingClause(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName == "sql_expression_list")
					GetHavingExpressions(childNode);
			}
		}

		private void GetHavingExpressions(ISqlNode node) {
			var exps = new List<IExpressionNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode)
					exps.Add((IExpressionNode) childNode);
			}

			HavingExpressions = exps.AsReadOnly();
		}

		private void GetGroupExpressions(ISqlNode node) {
			var exps = new List<IExpressionNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode)
					exps.Add((IExpressionNode) childNode);
			}

			GroupExpressions = exps.AsReadOnly();
		}
	}
}