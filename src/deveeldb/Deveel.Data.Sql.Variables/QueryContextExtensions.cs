using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Variables {
	public static class QueryContextExtensions {
		public static Variable ResolveVariable(this IQueryContext context, string variableName) {
			var queryContext = context;
			while (queryContext != null) {
				var variable = queryContext.VariableManager.GetVariable(variableName);
				if (variable != null)
					return variable;

				queryContext = queryContext.ParentContext;
			}

			return null;
		}
	}
}
