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
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Compile {
	static class TableConstraint {
		private static ForeignKeyAction GetForeignKeyAction(string text) {
			switch (text.ToUpperInvariant()) {
				case "SET NULL":
				case "SETNULL":
					return ForeignKeyAction.SetNull;
				case "SET DEFAULT":
				case "SETDEFAULT":
					return ForeignKeyAction.SetDefault;
				case "NO ACTION":
				case "NOACTION":
					return ForeignKeyAction.NoAction;
				case "CASCADE":
					return ForeignKeyAction.Cascade;
				default:
					throw new ParseCanceledException(String.Format("Invalid action '{0}' specified", text));
			}
		}

		public static SqlTableConstraint Form(PlSqlParser.TableConstraintContext context) {
			string constraintName = Name.Simple(context.id());

			ConstraintType type;
			string[] columns = null;
			string refTable = null;
			string[] refColumns = null;
			SqlExpression checkExp =  null;
			ForeignKeyAction? onDelete = null;
			ForeignKeyAction? onUpdate = null;

			if (context.primaryKeyConstraint() != null) {
				type = ConstraintType.PrimaryKey;

				columns = context.primaryKeyConstraint().columnList().columnName().Select(Name.Simple).ToArray();
			} else if (context.uniqueKeyConstraint() != null) {
				type = ConstraintType.Unique;
				columns = context.uniqueKeyConstraint().columnList().columnName().Select(Name.Simple).ToArray();
			} else if (context.checkConstraint() != null) {
				type = ConstraintType.Check;
				checkExp = Expression.Build(context.checkConstraint().expression());
			} else if (context.foreignKeyConstraint() != null) {
				type = ConstraintType.ForeignKey;
				columns = context.foreignKeyConstraint().columns.columnName().Select(Name.Simple).ToArray();
				refColumns = context.foreignKeyConstraint().refColumns.columnName().Select(Name.Simple).ToArray();
				refTable = Name.Object(context.foreignKeyConstraint().objectName()).ToString();
				var refActions = context.foreignKeyConstraint().referentialAction();
				if (refActions != null && refActions.Length > 0) {
					foreach (var action in refActions) {
						if (action.onDelete() != null) {
							var actionType = GetForeignKeyAction(action.onDelete().referentialActionType().GetText());
							onDelete = actionType;
						} else if (action.onUpdate() != null) {
							var actionType = GetForeignKeyAction(action.onUpdate().referentialActionType().GetText());
							onUpdate = actionType;
						}
					}
				}
			} else {
				throw new ParseCanceledException("Invalid ");
			}

			var constraint = new SqlTableConstraint(constraintName, type, columns);

			if (type == ConstraintType.ForeignKey) {
				constraint.ReferenceTable = refTable;
				constraint.ReferenceColumns = refColumns;
				constraint.OnUpdate = onUpdate ?? ForeignKeyAction.NoAction;
				constraint.OnDelete = onDelete ?? ForeignKeyAction.NoAction;
			} else if (type == ConstraintType.Check) {
				constraint.CheckExpression = checkExp;
			}

			return constraint;
		}
	}
}