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

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class AlterTableBuilder {
		public static SqlStatement Build(PlSqlParser.AlterTableStatementContext context) {
			var tableName = Name.Object(context.objectName());

			var actions = context.alterTableAction().Select(ActionBuilder.Build).ToArray();

			if (actions.Length == 1)
				return new AlterTableStatement(tableName, actions[0]);

			var list = new SequenceOfStatements();
			foreach (var action in actions) {
				list.Statements.Add(new AlterTableStatement(tableName, action));
			}

			return list;
		}

		static class ActionBuilder {
			public static IAlterTableAction Build(PlSqlParser.AlterTableActionContext context) {
				if (context.addColumnAction() != null) {
					var column = TableColumn.Form(context.addColumnAction().tableColumn(), new List<ColumnConstraint>());
					return new AddColumnAction(column);
				}
				if (context.addConstraintAction() != null) {
					var constraint = TableConstraint.Form(context.addConstraintAction().tableConstraint());
					return new AddConstraintAction(constraint);
				}

				if (context.dropColumnAction() != null) {
					var columnName = context.dropColumnAction().id().GetText();
					return new DropColumnAction(columnName);
				}

				if (context.dropConstraintAction() != null) {
					var constraintName = context.dropConstraintAction().regular_id().GetText();
					return new DropConstraintAction(constraintName);
				}

				if (context.dropPrimaryKeyAction() != null) {
					return new DropPrimaryKeyAction();
				}

				if (context.dropDefaultAction() != null) {
					var columnName = Name.Simple(context.dropDefaultAction().columnName());
					return new DropDefaultAction(columnName);
				}

				if (context.setDefaultAction() != null) {
					var columnName = Name.Simple(context.setDefaultAction().columnName());
					var defaultValue = Expression.Build(context.setDefaultAction().expressionWrapper());
					return new SetDefaultAction(columnName, defaultValue);
				}

				throw new ParseCanceledException("The ALTER TABLE action is not supported");
			}
		}
	}
}
