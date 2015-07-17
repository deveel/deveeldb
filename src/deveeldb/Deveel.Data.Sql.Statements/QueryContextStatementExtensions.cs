using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Deveel.Data.Sql.Statements {
	public static class QueryContextStatementExtensions {
		public static ITable[] ExecuteQuery(this IQueryContext context, SqlQuery query) {
			return StatementExecutor.Execute(context, query);
		}

		public static ITable[] ExecuteQuery(this IQueryContext context, string sqlSource, IEnumerable<QueryParameter> parameters) {
			var query = new SqlQuery(sqlSource);
			if (parameters != null) {
				foreach (var parameter in parameters) {
					query.Parameters.Add(parameter);
				}
			}

			return context.ExecuteQuery(query);
		}

		public static ITable[] ExecuteQuery(this IQueryContext context, string sqlSource, params QueryParameter[] parameters) {
			IEnumerable<QueryParameter> paramList = null;
			if (parameters != null) {
				paramList = parameters.AsEnumerable();
			}

			return context.ExecuteQuery(sqlSource, paramList);
		}

		public static ITable[] ExecuteQuery(this IQueryContext context, string sqlSource) {
			return context.ExecuteQuery(sqlSource, null);
		}

		// TODO: Provide an overload with dynamic object parameter
	}
}
