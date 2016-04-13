using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class SequenceStatements {
		public static SqlStatement Create(PlSqlParser.CreateSequenceStatementContext context) {
			throw new NotImplementedException();
		}

		public static SqlStatement Drop(PlSqlParser.DropSequenceStatementContext context) {
			var names = context.objectName().Select(Name.Object).ToArray();
			var ifExists = context.IF() != null && context.EXISTS() != null;

			if (names.Length == 1)
				return new DropSequenceStatement(names[0]);

			var sequence = new SequenceOfStatements();
			foreach (var name in names) {
				sequence.Statements.Add(new DropSequenceStatement(name));
			}

			return sequence;
		}
	}
}
