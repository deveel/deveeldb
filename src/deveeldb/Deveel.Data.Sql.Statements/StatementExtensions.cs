using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public static class StatementExtensions {
		public static ITable Execute(this IStatement statement, IQueryContext context) {
			return Execute(statement, null, context);
		}

		public static ITable Execute(this IStatement statement, IExpressionPreparer preparer, IQueryContext context) {
			if (statement is SqlStatement)
				return ((SqlStatement) statement).Evaluate(preparer, context);

			var prepared = statement.Prepare(preparer, context);
			return prepared.Execute(context);
		}
	}
}
