using System;

namespace Deveel.Data.Sql.Expressions.Build {
	public interface IExpressionBuilder {
		IExpressionBuilder Value(object value);

		IExpressionBuilder Binary(SqlExpressionType binaryType, Action<IExpressionBuilder> right);

		IExpressionBuilder Unary(SqlExpressionType unaryType);

		IExpressionBuilder Query(Action<IQueryExpressionBuilder> query);

		IExpressionBuilder Function(ObjectName functionName, params SqlExpression[] args);

		IExpressionBuilder Reference(ObjectName referenceName);

		IExpressionBuilder Variable(string variableName);

		IExpressionBuilder Quantified(SqlExpressionType quantifyType, Action<IExpressionBuilder> expression);
	}
}
