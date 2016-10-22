using System;

namespace Deveel.Data.Sql.Expressions {
	public interface IQueryExpressionSourceBuilder {
		IQueryExpressionSourceBuilder Table(ObjectName tableName);

		IQueryExpressionSourceBuilder Query(Action<IQueryExpressionBuilder> query);

		IQueryExpressionSourceBuilder As(string alias);

		IQueryExpressionSourceBuilder Join(Action<IQueryExpressionSourceJoinBuilder> join);
	}
}
