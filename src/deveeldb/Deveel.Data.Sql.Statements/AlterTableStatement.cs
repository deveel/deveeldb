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

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterTableStatement : SqlStatement {
		public AlterTableStatement(string tableName, IAlterTableAction action) {
			TableName = tableName;
			Action = action;
		}

		public string TableName { get; private set; }

		public IAlterTableAction Action { get; private set; }

		protected override IPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			if (String.IsNullOrEmpty(TableName))
				throw new StatementPrepareException("The table name is required.");

			var tableName = context.ResolveTableName(TableName);
			return new Prepared(this, tableName, Action);
		}

		#region PreparedAlterTableStatemet

		class Prepared : SqlPreparedStatement {
			internal Prepared(AlterTableStatement source, ObjectName tableName, IAlterTableAction action)
				: base(source) {
				TableName = tableName;
				Action = action;
			}

			public ObjectName TableName { get; private set; }

			public IAlterTableAction Action { get; private set; }

			private static void CheckColumnConstraint(string columnName, string[] columns, ObjectName table, string constraintName) {
				foreach (string column in columns) {
					if (columnName.Equals(column)) {
						throw new ConstraintViolationException(SqlModelErrorCodes.DropColumnViolation,
								  "Constraint violation (" + constraintName +
								  ") dropping column " + columnName + " because of " +
								  "referential constraint in " + table);
					}
				}

			}

			private bool CheckColumnNamesMatch(IQueryContext context, String col1, String col2) {
				var comparison = context.IgnoreIdentifiersCase() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
				return col1.Equals(col2, comparison);
			}

			protected override ITable ExecuteStatement(IQueryContext context) {
				if (!context.UserCanAlterTable(TableName))
					throw new InvalidAccessException(context.UserName(), TableName);

				var table = context.GetTable(TableName);
				if (table == null)
					throw new ObjectNotFoundException(TableName);

				var tableInfo = table.TableInfo;
				var newTableInfo = new TableInfo(tableInfo.TableName);

				var checker = ColumnChecker.Default(context, TableName);

				bool tableAltered = false;
				bool markDropped = false;

				for (int n = 0; n < tableInfo.ColumnCount; ++n) {
					var column = tableInfo[n];

					string columnName = column.ColumnName;
					var columnType = column.ColumnType;
					var defaultExpression = column.DefaultExpression;

					if (Action.ActionType == AlterTableActionType.SetDefault &&
					    CheckColumnNamesMatch(context, ((SetDefaultAction) Action).ColumnName, columnName)) {
						var exp = ((SetDefaultAction) Action).DefaultExpression;
						exp = checker.CheckExpression(exp);
						defaultExpression = exp;
						tableAltered = true;
					} else if (Action.ActionType == AlterTableActionType.DropDefault &&
					           CheckColumnNamesMatch(context, ((DropDefaultAction) Action).ColumnName, columnName)) {
						defaultExpression = null;
						tableAltered = true;
					} else if (Action.ActionType == AlterTableActionType.DropColumn &&
					           CheckColumnNamesMatch(context, ((DropColumnAction) Action).ColumnName, columnName)) {
						// Check there are no referential links to this column
						var refs = context.GetTableImportedForeignKeys(TableName);
						foreach (var reference in refs) {
							CheckColumnConstraint(columnName, reference.ForeignColumnNames, reference.ForeignTable, reference.ConstraintName);
						}

						// Or from it
						refs = context.GetTableForeignKeys(TableName);
						foreach (var reference in refs) {
							CheckColumnConstraint(columnName, reference.ColumnNames, reference.TableName, reference.ConstraintName);
						}

						// Or that it's part of a primary key
						var primaryKey = context.GetTablePrimaryKey(TableName);
						if (primaryKey != null)
							CheckColumnConstraint(columnName, primaryKey.ColumnNames, TableName, primaryKey.ConstraintName);

						// Or that it's part of a unique set
						var uniques = context.GetTableUniqueKeys(TableName);
						foreach (var unique in uniques) {
							CheckColumnConstraint(columnName, unique.ColumnNames, TableName, unique.ConstraintName);
						}

						markDropped = true;
						tableAltered = true;
					}

					var newColumn = new ColumnInfo(columnName, columnType);
					if (defaultExpression != null)
						newColumn.DefaultExpression = defaultExpression;

					newColumn.IndexType = column.IndexType;
					newColumn.IsNotNull = column.IsNotNull;

					// If not dropped then add to the new table definition.
					if (!markDropped) {
						newTableInfo.AddColumn(newColumn);
					}
				}

				if (Action.ActionType == AlterTableActionType.AddColumn) {
					var col = ((AddColumnAction)Action).Column;

					checker.CheckExpression(col.DefaultExpression);
					var columnName = col.ColumnName;
					var columnType = col.ColumnType;

					// If column name starts with [table_name]. then strip it off
					columnName = checker.StripTableName(TableName.Name, columnName);
					if (tableInfo.IndexOfColumn(col.ColumnName) != -1)
						throw new InvalidOperationException("The column '" + col.ColumnName + "' is already in the table '" + tableInfo.TableName + "'.");

					var newColumn = new ColumnInfo(columnName, columnType) {
						IsNotNull = col.IsNotNull,
						DefaultExpression = col.DefaultExpression
					};

					newTableInfo.AddColumn(newColumn);
					tableAltered = true;
				}

				if (Action.ActionType == AlterTableActionType.DropConstraint) {
					string constraintName = ((DropConstraintAction)Action).ConstraintName;
					int dropCount = context.DropConstraint(TableName, constraintName);
					if (dropCount == 0)
						throw new InvalidOperationException("Named constraint to drop on table " + TableName + " was not found: " + constraintName);
				} else if (Action.ActionType == AlterTableActionType.DropPrimaryKey) {
					string constraintName = ((DropConstraintAction)Action).ConstraintName;
					if (!context.DropPrimaryKey(TableName, constraintName))
						throw new InvalidOperationException("No primary key to delete on table " + TableName);
				}

				if (Action.ActionType == AlterTableActionType.AddConstraint) {
					var constraint = ((AddConstraintAction)Action).Constraint;
					bool foreignConstraint = (constraint.ConstraintType == ConstraintType.ForeignKey);

					ObjectName refTname = null;
					if (foreignConstraint) {
						refTname = context.ResolveTableName(constraint.ForeignTable);
					}

					var columnNames = checker.StripColumnList(TableName.FullName, constraint.ColumnNames);
					columnNames = checker.StripColumnList(constraint.ForeignTable.FullName, columnNames);
					var expression = checker.CheckExpression(constraint.CheckExpression);
					columnNames = checker.CheckColumns(columnNames);

					IEnumerable<string> refCols = null;
					if (foreignConstraint && constraint.ForeignColumnNames != null) {
						var referencedChecker = ColumnChecker.Default(context, refTname);
						refCols = referencedChecker.CheckColumns(constraint.ForeignColumnNames);
					}

					var newConstraint = new ConstraintInfo(constraint.ConstraintType, constraint.TableName, columnNames.ToArray());
					if (foreignConstraint) {
						newConstraint.ForeignTable = refTname;
						newConstraint.ForeignColumnNames = refCols.ToArray();
					}

					if (constraint.ConstraintType == ConstraintType.Check)
						newConstraint.CheckExpression = expression;

					context.AddConstraint(TableName, newConstraint);
				}

				// Alter the existing table to the new format...
				if (tableAltered) {
					if (newTableInfo.ColumnCount == 0)
						throw new InvalidOperationException("Can not ALTER table to have 0 columns.");

					context.AlterTable(newTableInfo);
				} else {
					// If the table wasn't physically altered, check the constraints.
					// Calling this method will also make the transaction check all
					// deferred constraints during the next commit.
					context.CheckConstraints(TableName);
				}

				// Return '0' if everything successful.
				return FunctionTable.ResultTable(context, 0);
			}
		}

		#endregion
	}
}
