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

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Parser {
	class TableConstraintNode : SqlNode, ITableElementNode {
		private bool notSeen;
		private readonly IList<string> columns;
		private readonly IList<string> refColumns;

		internal TableConstraintNode() {
			columns = new List<string>();
			refColumns = new List<string>();
		}

		public string ConstraintName { get; private set; }

		public string ConstraintType { get; private set; }

		public IEnumerable<string> Columns {
			get { return columns.AsEnumerable(); }
		}

		public IExpressionNode CheckExpression { get; private set; }

		public ObjectNameNode ReferencedTableName { get; private set; }

		public IEnumerable<string> ReferencedColumns {
			get { return refColumns.AsEnumerable(); }
		}

		public string OnUpdateAction { get; private set; }

		public string OnDeleteAction { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "table_constraint_name_opt") {
				ReadConstraintName(node.ChildNodes);
			} else if (node.NodeName == "def_table_constraint") {
				ReadConstraintDefinition(node);
			}

			return base.OnChildNode(node);
		}

		private void ReadConstraintDefinition(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					var keyNode = (SqlKeyNode) childNode;
					if (String.Equals(keyNode.Text, "NULL", StringComparison.OrdinalIgnoreCase)) {
						if (notSeen) {
							ConstraintType = "NOT NULL";
						} else {
							ConstraintType = "NULL";
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
				} else if (childNode.NodeName == "column_list") {
					ReadColumnList(childNode.ChildNodes);
				}
			}
		}

		private void ReadConstraintName(IEnumerable<ISqlNode> nodes) {
			foreach (var node in nodes) {
				if (node is IdentifierNode) {
					ConstraintName = ((IdentifierNode) node).Text;
				} else {
					ReadConstraintName(node.ChildNodes);
				}
			}
		}

		private void ReadColumnList(IEnumerable<ISqlNode> nodes) {
			foreach (var node in nodes) {
				if (node is IdentifierNode) {
					columns.Add(((IdentifierNode) node).Text);
				} else {
					ReadColumnList(node.ChildNodes);
				}
			}
		}

		public SqlTableConstraint BuildConstraint() {
			if (String.Equals(ConstraintTypeNames.Check, ConstraintType, StringComparison.OrdinalIgnoreCase)) {
				var exp = ExpressionBuilder.Build(CheckExpression);
				return new SqlTableConstraint(ConstraintName, Tables.ConstraintType.Check, Columns.ToArray()) {
					CheckExpression = exp
				};
			}
			if (String.Equals(ConstraintTypeNames.PrimaryKey, ConstraintType, StringComparison.OrdinalIgnoreCase))
				return SqlTableConstraint.PrimaryKey(ConstraintName, Columns.ToArray());
			if (String.Equals(ConstraintTypeNames.UniqueKey, ConstraintType, StringComparison.OrdinalIgnoreCase))
				return SqlTableConstraint.UniqueKey(ConstraintName, Columns.ToArray());
			if (String.Equals(ConstraintTypeNames.ForeignKey, ConstraintType, StringComparison.OrdinalIgnoreCase)) {
				var fTable = ReferencedTableName.Name;
				var fColumns = ReferencedColumns;
				var onDelete = ForeignKeyAction.NoAction;
				var onUpdate = ForeignKeyAction.NoAction;

				if (!String.IsNullOrEmpty(OnDeleteAction))
					onDelete = SqlCodeObjectBuilder.GetForeignKeyAction(OnDeleteAction);
				if (!String.IsNullOrEmpty(OnUpdateAction))
					onUpdate = SqlCodeObjectBuilder.GetForeignKeyAction(OnUpdateAction);

				var fkey = SqlTableConstraint.ForeignKey(ConstraintName, Columns.ToArray(), fTable,
					fColumns.ToArray(), onDelete, onUpdate);

				return fkey;
			}

			throw new NotSupportedException();
		}
	}
}