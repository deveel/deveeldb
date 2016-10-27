using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Statements.Build {
	public static class SqlStatementBuilder {
		public static IEnumerable<SqlStatement> CreateTable(Action<ICreateTableStatementBuilder> table) {
			var builder = new CreateTableStatementBuilder();
			table(builder);

			return builder.Build();
		}

		public static SelectStatement Select(Action<ISelectStatementBuilder> select) {
			var builder = new SelectStatementBuilder();
			select(builder);

			return builder.Build().OfType<SelectStatement>().FirstOrDefault();
		}
	}
}
