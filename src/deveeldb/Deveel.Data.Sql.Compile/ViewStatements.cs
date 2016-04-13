using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class ViewStatements {
		public static CreateViewStatement Create(PlSqlParser.CreateViewStatementContext context) {
			var orReplace = context.OR() != null && context.REPLACE() != null;

			var viewName = Name.Object(context.objectName());
			var query = (SqlQueryExpression) Expression.Build(context.subquery());

			string[] columnNames = null;
			if (context.columnList() != null) {
				columnNames = context.columnList().columnName().Select(Name.Simple).ToArray();
			}

			return new CreateViewStatement(viewName, columnNames, query) {
				ReplaceIfExists = orReplace
			};
		}

		public static SqlStatement Drop(PlSqlParser.DropViewStatementContext context) {
			var names = context.objectName().Select(Name.Object).ToArray();
			var ifExists = context.IF() != null && context.EXISTS() != null;

			if (names.Length == 1)
				return new DropViewStatement(names[0], ifExists);

			var sequence = new SequenceOfStatements();
			foreach (var name in names) {
				sequence.Statements.Add(new DropViewStatement(name, ifExists));
			}

			return sequence;
		}
	}
}
