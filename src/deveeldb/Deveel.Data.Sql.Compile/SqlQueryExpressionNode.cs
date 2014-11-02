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
	public sealed class SqlQueryExpressionNode : SqlNode, IExpressionNode {
		public bool IsAll { get; private set; }

		public bool IsDistinct { get; private set; }

		public bool SelectAll { get; private set; }

		public IEnumerable<SelectItemNode> SelectItems { get; private set; }

		public FromClauseNode FromClause { get; private set; }

		public IExpressionNode WhereExpression { get; private set; }

		public IExpressionNode HavingExpression { get; private set; }

		public IEnumerable<OrderByNode> OrderBy { get; private set; }

		public QueryCompositeNode Composite { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "select_restrict_opt") {
				GetRestrict(node);
			} else if (node.NodeName == "select_set") {
				GetSelectSet(node);
			} else if (node.NodeName == "from_clause_opt") {
				var clause = node.ChildNodes.FirstOrDefault();
				if (clause != null)
					FromClause = (FromClauseNode)clause;
			} else if (node.NodeName == "query_composite_opt") {
				var composite = node.ChildNodes.FirstOrDefault();
				if (composite != null)
					Composite = (QueryCompositeNode) composite;
			}

			return base.OnChildNode(node);
		}

		private void GetRestrict(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					var value = ((SqlKeyNode) childNode).Text;
					if (value == "ALL") {
						IsAll = true;
					} else if (value == "DISTINCT") {
						IsDistinct = true;
					}
				}
			}
		}

		private void GetSelectSet(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode &&
				    ((SqlKeyNode) childNode).Text == "*") {
					SelectAll = true;
				} else if (childNode.NodeName == "select_item_list") {
					GetSelectItems(childNode);
				}
			}
		}

		private void GetSelectItems(ISqlNode node) {
			var items = new List<SelectItemNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SelectItemNode)
					items.Add((SelectItemNode)childNode);
			}

			SelectItems = items.AsReadOnly();
		}
	}
}