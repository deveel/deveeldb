using System;

namespace Deveel.Data.Sql.Statements {
	public abstract class SqlPreparableStatement : SqlStatement, IPreparableStatement {
		IPreparedStatement IPreparableStatement.Prepare(IRequest context) {
			return PrepareStatement(context);
		}

		protected abstract IPreparedStatement PrepareStatement(IRequest request);
	}
}
