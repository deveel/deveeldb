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

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterTableStatement : SqlStatement {
		public override StatementType StatementType {
			get { return StatementType.AlterTable;}
		}

		public string TableName { get; set; }

		public IAlterTableAction Action { get; set; }

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
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
