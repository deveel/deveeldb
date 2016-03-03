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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Parser {
	class TableColumnNode : SqlNode, ITableElementNode {
		public string ColumnName { get; private set; }

		public DataTypeNode DataType { get; private set; }

		public IEnumerable<ColumnConstraintNode> Constraints { get; private set; }

		public IExpressionNode Default { get; private set; }

		public bool IsIdentity { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				ColumnName = ((IdentifierNode) node).Text;
			} else if (node is DataTypeNode) {
				DataType = ((DataTypeNode) node);
			} else if (node.NodeName.Equals("column_default_or_identity_opt")) {
				GetDefaultOrIdentity(node);
			} else if (node.NodeName.Equals("column_constraint_list")) {
				Constraints = node.FindNodes<ColumnConstraintNode>();
			}

			return base.OnChildNode(node);
		}

		private void GetDefaultOrIdentity(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					var keyNode = (SqlKeyNode) childNode;
					if (keyNode.Text.Equals("IDENTITY", StringComparison.OrdinalIgnoreCase)) {
						IsIdentity = true;
						break;
					}
				} else if (childNode is IExpressionNode) {
					Default = (IExpressionNode) childNode;
				}
			}
		}

		public SqlTableColumn BuildColumn(ITypeResolver typeResolver, string tableName, IList<SqlTableConstraint> constraints) {
			var dataType = DataTypeBuilder.Build(typeResolver, DataType);

			var columnInfo = new SqlTableColumn(ColumnName, dataType);

			if (Default != null)
				columnInfo.DefaultExpression = ExpressionBuilder.Build(Default);

			if (IsIdentity) {
				columnInfo.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
					new[] { SqlExpression.Constant(tableName) });
				columnInfo.IsIdentity = true;
			}

			foreach (var constraint in Constraints) {
				if (String.Equals(ConstraintTypeNames.Check, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					var exp = ExpressionBuilder.Build(constraint.CheckExpression);
					constraints.Add(SqlTableConstraint.Check(null, exp));
				} else if (String.Equals(ConstraintTypeNames.ForeignKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					var fTable = constraint.ReferencedTable.Name;
					var fColumn = constraint.ReferencedColumn.Text;
					var onDelete = ForeignKeyAction.NoAction;
					var onUpdate = ForeignKeyAction.NoAction;
					
					if (!String.IsNullOrEmpty(constraint.OnDeleteAction))
						onDelete = SqlCodeObjectBuilder.GetForeignKeyAction(constraint.OnDeleteAction);
					if (!String.IsNullOrEmpty(constraint.OnUpdateAction))
						onUpdate = SqlCodeObjectBuilder.GetForeignKeyAction(constraint.OnUpdateAction);

					constraints.Add(SqlTableConstraint.ForeignKey(null, new[]{ColumnName}, fTable, new[]{fColumn}, onDelete, onUpdate));
				} else if (String.Equals(ConstraintTypeNames.PrimaryKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					constraints.Add(SqlTableConstraint.PrimaryKey(null, new[]{ColumnName}));
				} else if (String.Equals(ConstraintTypeNames.UniqueKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					constraints.Add(SqlTableConstraint.UniqueKey(null, new[]{ColumnName}));
				} else if (String.Equals(ConstraintTypeNames.NotNull, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					columnInfo.IsNotNull = true;
				} else if (String.Equals(ConstraintTypeNames.Null, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					columnInfo.IsNotNull = false;
				}
			}

			return columnInfo;
		}
	}
}
