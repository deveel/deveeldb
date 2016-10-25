using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Expressions.Build;

namespace Deveel.Data.Sql.Statements.Build {
	public interface ISelectStatementBuilder {
		ISelectStatementBuilder Query(SqlQueryExpression expression);

		ISelectStatementBuilder OrderBy(SortColumn sort);

		ISelectStatementBuilder Limit(QueryLimit limit);
	}
}
