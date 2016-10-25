using System;

namespace Deveel.Data.Sql.Expressions.Build {
	public interface IQueryExpressionItemBuilder {
		IQueryExpressionItemBuilder Expression(SqlExpression expression);

		IQueryExpressionItemBuilder As(string alias);
	}
}
