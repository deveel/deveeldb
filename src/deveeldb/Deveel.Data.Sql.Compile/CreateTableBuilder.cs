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

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class CreateTableBuilder {
		public static SqlStatement Build(PlSqlParser.CreateTableStatementContext context) {
			var tableName = Name.Object(context.objectName());
			var columnOrConstraints = context.columnOrConstraintList().columnOrConstraint();

			bool ifNotExists = context.IF() != null && context.NOT() != null && context.EXISTS() != null;
			bool temporary = context.TEMPORARY() != null;

			var tableColumns = new List<SqlTableColumn>();
			var tableConstraints = new List<SqlTableConstraint>();
			var columnConstraints = new List<ColumnConstraint>();

			foreach (var columnOrConstraint in columnOrConstraints) {
				if (columnOrConstraint.tableColumn() != null) {
					tableColumns.Add(TableColumn.Form(columnOrConstraint.tableColumn(), columnConstraints));
				} else if (columnOrConstraint.tableConstraint() != null) {
					tableConstraints.Add(TableConstraint.Form(columnOrConstraint.tableConstraint()));
				}
			}

			if (columnConstraints.Count > 0) {
				var constraintGroups = columnConstraints.GroupBy(x => x.Type);

				foreach (var constraintGroup in constraintGroups) {
					var columnNames = constraintGroup.Select(x => x.ColumnName).ToArray();
					var index = tableConstraints.FindIndex(x => String.IsNullOrEmpty(x.ConstraintName) &&
					                                     x.ConstraintType == constraintGroup.Key);
					if (index != -1) {
						var unnamedConstraint = tableConstraints[index];
						var columns = new List<string>(unnamedConstraint.Columns);
						foreach (var columnName in columnNames) {
							if (!columns.Contains(columnName))
								columns.Add(columnName);
						}

						// TODO: set the new columns
					} else {
						tableConstraints.Add(new SqlTableConstraint(constraintGroup.Key, columnNames));
					}
				}
			}

			var block = new SequenceOfStatements();
			block.Statements.Add(new CreateTableStatement(tableName, tableColumns) {
				IfNotExists = ifNotExists,
				Temporary = temporary
			});

			foreach (var constraint in tableConstraints) {
				block.Statements.Add(new AlterTableStatement(tableName, new AddConstraintAction(constraint)));
			}

			return block;
		}
	}
}
