using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Statements {
	interface IParentExecutable : IExecutable {
		IEnumerable<SqlStatement> Children { get; }
	}
}
