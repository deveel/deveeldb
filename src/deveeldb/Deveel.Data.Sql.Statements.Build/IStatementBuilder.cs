using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Statements.Build {
	interface IStatementBuilder {
		IEnumerable<SqlStatement> Build();
	}
}
