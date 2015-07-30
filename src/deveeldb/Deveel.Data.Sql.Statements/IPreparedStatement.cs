using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	public interface IPreparedStatement {
		IStatement Source { get; }


		ITable Execute(IQueryContext context);
	}
}
