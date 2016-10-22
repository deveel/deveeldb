using System;

namespace Deveel.Data.Sql.Expressions {
	public interface IQueryExpressionBuilder {
		IQueryExpressionBuilder Item(Action<IQueryExpressionItemBuilder> item);

		IQueryExpressionBuilder From(Action<IQueryExpressionSourceBuilder> source);

		IQueryExpressionBuilder GroupBy(params SqlExpression[] groupBy);

		IQueryExpressionBuilder GroupMax(ObjectName columnName);

		IQueryExpressionBuilder Where(SqlExpression where);

		IQueryExpressionBuilder Having(SqlExpression having);
	}
}
