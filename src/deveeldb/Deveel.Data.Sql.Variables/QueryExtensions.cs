using System;

namespace Deveel.Data.Sql.Variables {
	public static class QueryExtensions {
		public static Variable DeclareVariable(this IQuery query, VariableInfo variableInfo) {
			return query.QueryContext.DeclareVariable(variableInfo);
		}

		public static void DropVariable(this IQuery query, string variableName) {
			query.QueryContext.DropVariable(variableName);
		}
	}
}
