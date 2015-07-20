// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
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

		#region CreateView

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, string querySource) {
			return ExecuteCreateView(context, viewName, new string[0], querySource);
		}

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, IEnumerable<string> columnNames, string querySource) {
			var expression = SqlExpression.Parse(querySource);
			if (expression.ExpressionType != SqlExpressionType.Query)
				throw new ArgumentException("The input query string is invalid.", "querySource");

			return context.ExecuteCreateView(viewName, columnNames, (SqlQueryExpression) expression);
		}

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName,
			SqlQueryExpression queryExpression) {
			return ExecuteCreateView(context, viewName, new string[0], queryExpression);
		}

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, IEnumerable<string> columnNames,
			SqlQueryExpression queryExpression) {
			var statement = new CreateViewStatement(viewName, columnNames, queryExpression);
			return statement.Evaluate(context);
		}

		#endregion
	}
}
