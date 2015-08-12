using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	interface IExecutable {
		ITable Execute(IQueryContext context);
	}
}
