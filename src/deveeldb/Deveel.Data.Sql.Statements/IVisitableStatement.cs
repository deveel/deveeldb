using System;

namespace Deveel.Data.Sql.Statements {
	public interface IVisitableStatement {
		SqlStatement Accept(StatementVisitor visitor);
	}
}
