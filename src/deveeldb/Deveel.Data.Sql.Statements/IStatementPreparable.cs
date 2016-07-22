using System;

namespace Deveel.Data.Sql.Statements {
	public interface IStatementPreparable {
		object Prepare(IRequest context);
	}
}
