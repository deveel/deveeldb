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
	[Serializable]
	public sealed class CreateTableStatement : SqlStatement {
		public CreateTableStatement(ObjectName tableName, IEnumerable<SqlTableColumn> columns) {
			TableName = tableName;
			Columns = new List<SqlTableColumn>();
			if (columns != null) {
				foreach (var column in columns) {
					Columns.Add(column);
				}
			}
		}

		public ObjectName TableName { get; private set; }

		public IList<SqlTableColumn> Columns { get; private set; }

		public bool IfNotExists { get; set; }

		public bool Temporary { get; set; }

		public override StatementType StatementType {
			get { return StatementType.CreateTable; }
		}

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var tableInfo = CreateTableInfo(context);

			return new PreparedCreateTableStatement(tableInfo, IfNotExists, Temporary);
		}

		private TableInfo CreateTableInfo(IQueryContext context) {
			var tableName = TableName;

			tableName = context.ResolveTableName(tableName);

			var idColumnCount = Columns.Count(x => x.IsIdentity);
			if (idColumnCount > 1)
				throw new InvalidOperationException("More than one IDENTITY column specified.");

			bool ignoreCase = context.IgnoreIdentifiersCase();
			var columnChecker = new TableColumnChecker(Columns, ignoreCase);


			var tableInfo = new TableInfo(tableName);

			foreach (var column in Columns) {
				var columnInfo = CreateColumnInfo(tableName.Name, column, columnChecker);
				tableInfo.AddColumn(columnInfo);
			}

			return tableInfo;
		}

		private ColumnInfo CreateColumnInfo(string tableName, SqlTableColumn column, TableColumnChecker columnChecker) {
			var expression = column.DefaultExpression;

			if (column.IsIdentity && expression != null)
				throw new InvalidOperationException(String.Format("Identity column '{0}' cannot define a DEFAULT expression.", column.ColumnName));

			if (expression != null)
				expression = columnChecker.CheckExpression(expression);


			var columnName = columnChecker.StripTableName(tableName, column.ColumnName);

			return new ColumnInfo(columnName, column.ColumnType) {
				DefaultExpression = expression,
				IsNotNull = column.IsNotNull
			};
		}

		#region PreparedCreateTableStatement

		[Serializable]
		class PreparedCreateTableStatement : SqlPreparedStatement {
			private readonly TableInfo tableInfo;
			private readonly bool temporary;
			private readonly bool ifNotExists;

			public PreparedCreateTableStatement(TableInfo tableInfo, bool ifNotExists, bool temporary) {
				this.tableInfo = tableInfo;
				this.ifNotExists = ifNotExists;
				this.temporary = temporary;
			}

			public override ITable Evaluate(IQueryContext context) {
				if (!context.UserCanCreateTable(tableInfo.TableName))
					throw new MissingPrivilegesException(tableInfo.TableName,
						String.Format("User '{0}' has not enough privileges to create table '{1}'", context.User().Name, tableInfo.TableName));

				try {
					context.CreateTable(tableInfo, ifNotExists, temporary);

					using (var systemContext = new SystemQueryContext(context.Session.Transaction, context.CurrentSchema)) {
						systemContext.GrantToUserOnTable(tableInfo.TableName, Privileges.TableAll);
					}
					return FunctionTable.ResultTable(context, 0);
				} catch (SecurityException ex) {
					throw;
				} catch (Exception ex) {
					// TODO: Send a specialized error
					throw;
				}
			}
		}

		#endregion

		#region TableColumnChecker

		class TableColumnChecker : ColumnChecker {
			private readonly IEnumerable<SqlTableColumn> columns;
			private readonly bool ignoreCase;

			public TableColumnChecker(IEnumerable<SqlTableColumn> columns, bool ignoreCase) {
				this.columns = columns;
				this.ignoreCase = ignoreCase;
			}

			public override string ResolveColumnName(string columnName) {
				var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
				string foundColumn = null;

				foreach (var columnInfo in columns) {
					if (foundColumn != null)
						throw new InvalidOperationException(String.Format("Column name '{0}' caused an ambiguous match in table.", columnName));

					if (String.Equals(columnInfo.ColumnName, columnName, comparison))
						foundColumn = columnInfo.ColumnName;
				}

				return foundColumn;
			}
		}

		#endregion
	}
}
