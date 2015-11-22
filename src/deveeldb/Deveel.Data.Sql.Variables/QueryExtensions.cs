using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Variables {
	public static class QueryExtensions {
		public static Variable DeclareVariable(this IQuery query, VariableInfo variableInfo) {
			return query.QueryContext.DeclareVariable(variableInfo);
		}

		public static void DropVariable(this IQuery query, string variableName) {
			query.QueryContext.DropVariable(variableName);
		}

		public static Variable SetVariable(this IQuery query, string variableName, SqlExpression value) {
			return query.QueryContext.SetVariable(variableName, value);
		}

		public static Variable FindVariable(this IQuery query, string variableName) {
			return query.QueryContext.FindVariable(variableName);
		}

		public static Variable DeclareVariable(this IQuery query, string variableName, SqlType variableType) {
			return DeclareVariable(query, variableName, variableType, false);
		}

		public static Variable DeclareVariable(this IQuery query, string variableName, SqlType variableType, bool constant) {
			return query.DeclareVariable(new VariableInfo(variableName, variableType, constant));
		}
	}
}
