using System;

namespace Deveel.Data.Sql.Expressions.Build {
	public interface IQueryExpressionBuilder {
		IQueryExpressionBuilder Distinct(bool value = true);

		IQueryExpressionBuilder Item(Action<IQueryExpressionItemBuilder> item);

		IQueryExpressionBuilder From(params Action<IQueryExpressionSourceBuilder>[] source);

		IQueryExpressionBuilder GroupBy(params SqlExpression[] groupBy);

		IQueryExpressionBuilder GroupMax(ObjectName columnName);

		IQueryExpressionBuilder Where(SqlExpression where);

		IQueryExpressionBuilder Having(SqlExpression having);
	}
}
