using System;

namespace Deveel.Data.Sql.Statements {
	public interface IPreparableStatement : IStatement {
		IPreparedStatement Prepare(IRequest request);
	}
}
