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
	class CreateTableNode : SqlNode, IStatementNode {
		private readonly ICollection<TableColumnNode> columns;
		private readonly ICollection<TableConstraintNode> constraints;
 
		public CreateTableNode() {
			columns = new List<TableColumnNode>();
			constraints = new List<TableConstraintNode>();
		}

		public ObjectName TableName { get; private set; }

		public bool IfNotExists { get; private set; }

		public bool Temporary { get; private set; }

		public IEnumerable<TableColumnNode> Columns {
			get { return columns.AsEnumerable(); }
		}

		public IEnumerable<TableConstraintNode> Constraints {
			get { return constraints.AsEnumerable(); }
		}

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName == "if_not_exists_opt") {
				IfNotExists = true;
			} else if (node.NodeName == "temporary_opt") {
				Temporary = true;
			} else if (node.NodeName == "column_or_constraint_list") {
				ReadColumnOrConstraints(node.ChildNodes);
			}

			return base.OnChildNode(node);
		}

		private void ReadColumnOrConstraints(IEnumerable<ISqlNode> nodes) {
			foreach (var childNode in nodes) {
				if (childNode is TableColumnNode) {
					columns.Add(childNode as TableColumnNode);
				} else if (childNode is TableConstraintNode) {
					constraints.Add(childNode as TableConstraintNode);
				} else {
					ReadColumnOrConstraints(childNode.ChildNodes);
				}
			}
		}
	}
}
