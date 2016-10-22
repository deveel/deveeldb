using System;

namespace Deveel.Data.Sql.Expressions {
	public interface IQueryExpressionSourceJoinBuilder {
		IQueryExpressionSourceJoinBuilder Source(Action<IQueryExpressionSourceBuilder> source);

		IQueryExpressionSourceJoinBuilder JoinType(JoinType joinType);

		IQueryExpressionSourceJoinBuilder On(SqlExpression expression);
	}
}
