using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Statements {
	public interface IProgramSatement : IStatement {
		IEnumerable<IStatement> Body { get; }
	}
}
