using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterTableStatement : SqlStatement {
		public override StatementType StatementType {
			get { return StatementType.AlterTable;}
		}

		public string TableName { get; set; }

		public IAlterTableAction Action { get; set; }

		protected override SqlPreparedStatement PrepareStatement(IQueryContext context) {
			if (String.IsNullOrEmpty(TableName))
				throw new StatementPrepareException("The table name is required.");

			var tableName = context.ResolveTableName(TableName);
			return new PreparedAlterTableStatement(tableName, Action);
		}

		#region PreparedAlterTableStatemet

		class PreparedAlterTableStatement : SqlPreparedStatement {
			public PreparedAlterTableStatement(ObjectName tableName, IAlterTableAction action) {
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

			public override ITable Evaluate(IQueryContext context) {
				if (!context.UserCanAlterTable(TableName))
					throw new InvalidAccessException(TableName);

				var table = context.GetTable(TableName);
				if (table == null)
					throw new ObjectNotFoundException(TableName);

				var tableInfo = table.TableInfo;
				var newTableInfo = new TableInfo(tableInfo.TableName);

				var checker = ColumnChecker.Default(context, TableName);

				// Set to true if the table topology is alter, or false if only
				// the constraints are changed.
				bool tableAltered = false;

				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
