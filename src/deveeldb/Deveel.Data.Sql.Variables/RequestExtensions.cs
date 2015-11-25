using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Variables {
	public static class RequestExtensions {
		public static void SetVariable(this IRequest request, string variableName, SqlExpression value) {
			request.Context.SetVariable(variableName, value);
		}

		public static Variable FindVariable(this IRequest query, string variableName) {
			return query.Context.FindVariable(variableName);
		}
	}
}
