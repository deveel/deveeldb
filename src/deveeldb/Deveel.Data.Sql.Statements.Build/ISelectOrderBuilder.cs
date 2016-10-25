using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements.Build {
	public interface ISelectOrderBuilder {
		ISelectOrderBuilder Expression(SqlExpression expression);

		ISelectOrderBuilder Direction(SortDirection direction);
	}
}
