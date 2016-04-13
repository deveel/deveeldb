using System;
using System.Collections.Generic;
using System.Linq;

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
					var columnName = Name.Simple(context.dropDefaultAction().id());
					return new DropDefaultAction(columnName);
				}

				if (context.setDefaultAction() != null) {
					var columnName = Name.Simple(context.setDefaultAction().id());
					var defaultValue = Expression.Build(context.setDefaultAction().expression_wrapper());
					return new SetDefaultAction(columnName, defaultValue);
				}

				throw new NotSupportedException();
			}
		}
	}
}
