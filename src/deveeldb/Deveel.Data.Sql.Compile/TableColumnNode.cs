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
	[Serializable]
	class TableColumnNode : SqlNode {
		private readonly ICollection<ColumnConstraintNode> constraints;

		public TableColumnNode() {
			constraints = new List<ColumnConstraintNode>();
		}

		public string ColumnName { get; private set; }

		public DataTypeNode DataType { get; private set; }

		public IEnumerable<ColumnConstraintNode> Constraints {
			get { return constraints.AsEnumerable(); }
		}

		public IExpressionNode Default { get; private set; }

		public bool IsIdentity { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "column_name") {
				ColumnName = ((IdentifierNode) node.ChildNodes.First()).Text;
			} else if (node is DataTypeNode) {
				DataType = (DataTypeNode) node;
			} else if (node.NodeName == "column_constraint_list") {
				ReadColumnConstraints(node.ChildNodes);
			} else if (node.NodeName == "column_default_opt") {
				ReadDefaultExpression(node.ChildNodes);
			}

			return base.OnChildNode(node);
		}

		private void ReadDefaultExpression(IEnumerable<ISqlNode> nodes) {
			foreach (var child in nodes) {
				if (child is IExpressionNode) {
					Default = (IExpressionNode) child;
				} else {
					ReadDefaultExpression(child.ChildNodes);
				}
			}
		}

		private void ReadColumnConstraints(IEnumerable<ISqlNode> childNodes) {
			foreach (var childNode in childNodes) {
				if (childNode is ColumnConstraintNode) {
					constraints.Add(childNode as ColumnConstraintNode);
				} else {
					ReadColumnConstraints(childNode.ChildNodes);
				}
			}
		}
	}
}
