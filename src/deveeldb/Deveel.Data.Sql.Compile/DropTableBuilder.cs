using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class DropTableBuilder {
		public static SqlStatement Build(PlSqlParser.DropTableStatementContext context) {
			var tableNames = context.objectName().Select(Name.Object).ToArray();
			bool ifExists = context.IF() != null && context.EXISTS() != null;

			if (tableNames.Length == 1)
				return new DropTableStatement(tableNames[0], ifExists);

			var list = new SequenceOfStatements();
			foreach (var tableName in tableNames) {
				list.Statements.Add(new DropTableStatement(tableName, ifExists));
			}

			return list;
		}
	}
}
