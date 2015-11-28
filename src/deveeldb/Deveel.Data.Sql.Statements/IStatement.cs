using System;

namespace Deveel.Data.Sql.Statements {
	public interface IStatement {
		SqlQuery SourceQuery { get; }
	}
}
