using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class RequestExtensions {
		public static ITable ExecuteStatement(this IRequest request, IStatement statement) {
			var context = new ExecutionContext(request);

			if (statement is IPreparableStatement)
				statement = ((IPreparableStatement) statement).Prepare(request);

			statement.Execute(context);

			if (context.HasResult)
				return context.Result;

			return FunctionTable.ResultTable(request, 0);
		}
	}
}
