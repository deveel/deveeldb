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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AlterTableStatement : SqlStatement {
		public AlterTableStatement(ObjectName tableName, IAlterTableAction action) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (action == null)
				throw new ArgumentNullException("action");

			TableName = tableName;
			Action = action;
		}

		private AlterTableStatement(SerializationInfo info, StreamingContext context) {
			TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
			Action = (IAlterTableAction)info.GetValue("Action", typeof(IAlterTableAction));
		}

		public ObjectName TableName { get; private set; }

		public IAlterTableAction Action { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access().ResolveTableName(TableName);
			var action = Action;
			if (action is IStatementPreparable)
				action = (IAlterTableAction) ((IStatementPreparable) Action).Prepare(context);

			return new AlterTableStatement(tableName, action);
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var action = Action;
			if (action is IPreparable)
				action = (IAlterTableAction)(action as IPreparable).Prepare(preparer);

			return new AlterTableStatement(TableName, action);
		}

		private void CheckColumnConstraint(string columnName, string[] columns, ObjectName table, string constraintName) {
			foreach (string column in columns) {
				if (columnName.Equals(column)) {
					throw new DropColumnViolationException(TableName, columnName, constraintName, table);
				}
			}

		}

		private bool CheckColumnNamesMatch(IRequest context, String col1, String col2) {
			var comparison = context.Query.IgnoreIdentifiersCase() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return col1.Equals(col2, comparison);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TableName", TableName);
			info.AddValue("Action", Action);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanAlterTable(TableName))
				throw new InvalidAccessException(context.Request.UserName(), TableName);

			var table = context.Request.Access().GetTable(TableName);
			if (table == null)
				throw new ObjectNotFoundException(TableName);

			var tableInfo = table.TableInfo;
			var newTableInfo = new TableInfo(tableInfo.TableName);

			var checker = ColumnChecker.Default(context.Request, TableName);

			bool tableAltered = false;
			bool markDropped = false;

			for (int n = 0; n < tableInfo.ColumnCount; ++n) {
				var column = tableInfo[n];

				string columnName = column.ColumnName;
				var columnType = column.ColumnType;
				var defaultExpression = column.DefaultExpression;

				if (Action.ActionType == AlterTableActionType.SetDefault &&
					CheckColumnNamesMatch(context.Request, ((SetDefaultAction)Action).ColumnName, columnName)) {
					var exp = ((SetDefaultAction)Action).DefaultExpression;
					exp = checker.CheckExpression(exp);
					defaultExpression = exp;
					tableAltered = true;
				} else if (Action.ActionType == AlterTableActionType.DropDefault &&
						   CheckColumnNamesMatch(context.Request, ((DropDefaultAction)Action).ColumnName, columnName)) {
					defaultExpression = null;
					tableAltered = true;
				} else if (Action.ActionType == AlterTableActionType.DropColumn &&
						   CheckColumnNamesMatch(context.Request, ((DropColumnAction)Action).ColumnName, columnName)) {
					// Check there are no referential links to this column
					var refs = context.Request.Access().QueryTableImportedForeignKeys(TableName);
					foreach (var reference in refs) {
						CheckColumnConstraint(columnName, reference.ForeignColumnNames, reference.ForeignTable, reference.ConstraintName);
					}

					// Or from it
					refs = context.Request.Access().QueryTableForeignKeys(TableName);
					foreach (var reference in refs) {
						CheckColumnConstraint(columnName, reference.ColumnNames, reference.TableName, reference.ConstraintName);
					}

					// Or that it's part of a primary key
					var primaryKey = context.Request.Access().QueryTablePrimaryKey(TableName);
					if (primaryKey != null)
						CheckColumnConstraint(columnName, primaryKey.ColumnNames, TableName, primaryKey.ConstraintName);

					// Or that it's part of a unique set
					var uniques = context.Request.Access().QueryTableUniqueKeys(TableName);
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
				int dropCount = context.Request.Access().DropTableConstraint(TableName, constraintName);
				if (dropCount == 0)
					throw new InvalidOperationException("Named constraint to drop on table " + TableName + " was not found: " + constraintName);
			} else if (Action.ActionType == AlterTableActionType.DropPrimaryKey) {
				if (!context.Request.Access().DropTablePrimaryKey(TableName, null))
					throw new InvalidOperationException("No primary key to delete on table " + TableName);
			}

			if (Action.ActionType == AlterTableActionType.AddConstraint) {
				var constraint = ((AddConstraintAction)Action).Constraint;
				bool foreignConstraint = (constraint.ConstraintType == ConstraintType.ForeignKey);

				ObjectName refTname = null;
				if (foreignConstraint) {
					refTname = context.Request.Access().ResolveTableName(constraint.ReferenceTable);
				}

				var columnNames = checker.StripColumnList(TableName.FullName, constraint.Columns);
				columnNames = checker.StripColumnList(constraint.ReferenceTable, columnNames);
				var expression = checker.CheckExpression(constraint.CheckExpression);
				columnNames = checker.CheckColumns(columnNames);

				IEnumerable<string> refCols = null;
				if (foreignConstraint && constraint.ReferenceColumns != null) {
					var referencedChecker = ColumnChecker.Default(context.Request, refTname);
					refCols = referencedChecker.CheckColumns(constraint.ReferenceColumns);
				}

				var newConstraint = new ConstraintInfo(constraint.ConstraintName, constraint.ConstraintType, TableName, columnNames.ToArray());
				if (foreignConstraint) {
					if (refCols == null)
						throw new InvalidOperationException("Could not create a Foreign Key constraint with no reference columns");

					newConstraint.ForeignTable = refTname;
					newConstraint.ForeignColumnNames = refCols.ToArray();
					newConstraint.OnDelete = constraint.OnDelete;
					newConstraint.OnUpdate = constraint.OnUpdate;
				}

				if (constraint.ConstraintType == ConstraintType.Check)
					newConstraint.CheckExpression = expression;

				context.Request.Access().AddConstraint(TableName, newConstraint);
			}

			// Alter the existing table to the new format...
			if (tableAltered) {
				if (newTableInfo.ColumnCount == 0)
					throw new InvalidOperationException("Can not ALTER table to have 0 columns.");

				context.DirectAccess.AlterObject(newTableInfo);
			} else {
				// If the table wasn't physically altered, check the constraints.
				// Calling this method will also make the transaction check all
				// deferred constraints during the next commit.
				context.Request.Access().CheckConstraintViolations(TableName);
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat("ALTER TABLE {0} ", TableName);
			Action.AppendTo(builder);
		}
	}
}
