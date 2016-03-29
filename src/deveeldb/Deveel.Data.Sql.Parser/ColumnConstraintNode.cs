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

namespace Deveel.Data.Sql.Parser {
	class ColumnConstraintNode : SqlNode {
		private bool notSeen;

		internal ColumnConstraintNode() {
		}

		public string ConstraintType { get; private set; }

		public IExpressionNode CheckExpression { get; private set; }

		public ObjectNameNode ReferencedTable { get; private set; }

		public IdentifierNode ReferencedColumn { get; private set; }

		public string OnDeleteAction { get; private set; }

		public string OnUpdateAction { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {
				var keyNode = (SqlKeyNode) node;
				if (String.Equals(keyNode.Text, "NULL", StringComparison.OrdinalIgnoreCase)) {
					if (notSeen) {
						ConstraintType = ConstraintTypeNames.NotNull;
					} else {
						ConstraintType = ConstraintTypeNames.Null;
					}
				} else if (String.Equals(keyNode.Text, "NOT", StringComparison.OrdinalIgnoreCase)) {
					notSeen = true;
				} else if (String.Equals(keyNode.Text, "REFERENCES", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.ForeignKey;
				} else if (String.Equals(keyNode.Text, "CHECK", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.Check;
				} else if (String.Equals(keyNode.Text, "PRIMARY", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.PrimaryKey;
				} else if (String.Equals(keyNode.Text, "UNIQUE", StringComparison.OrdinalIgnoreCase)) {
					ConstraintType = ConstraintTypeNames.UniqueKey;
				}
			} else if (node is IExpressionNode) {
				CheckExpression = (IExpressionNode) node;
			} else if (node is ObjectNameNode) {
				if (!String.Equals(ConstraintType, ConstraintTypeNames.ForeignKey, StringComparison.OrdinalIgnoreCase))
					throw new InvalidOperationException();

				ReferencedTable = ((ObjectNameNode) node);
			}

			return base.OnChildNode(node);
		}
	}
}
