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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class QueryExtensions {
		public static ITable[] ExecuteQuery(this IQuery query, SqlQuery sqlQuery) {
			return query.Execute(sqlQuery);
		}

		public static ITable[] ExecuteQuery(this IQuery query, string sqlSource, params QueryParameter[] parameters) {
			var sqlQuery = new SqlQuery(sqlSource);
			if (parameters != null) {
				foreach (var parameter in parameters) {
					sqlQuery.Parameters.Add(parameter);
				}
			}

			return query.ExecuteQuery(sqlQuery);
		}

		public static ITable[] ExecuteQuery(this IQuery query, string sqlSource) {
			return query.ExecuteQuery(sqlSource, null);
		}

		// TODO: Provide an overload with dynamic object parameter

		#region CreateView

		public static ITable ExecuteCreateView(this IQuery query, string viewName, string querySource) {
			return ExecuteCreateView(query, viewName, new string[0], querySource);
		}

		public static ITable ExecuteCreateView(this IQuery query, string viewName, IEnumerable<string> columnNames, string querySource) {
			var expression = SqlExpression.Parse(querySource);
			if (expression.ExpressionType != SqlExpressionType.Query)
				throw new ArgumentException("The input query string is invalid.", "querySource");

			return query.ExecuteCreateView(viewName, columnNames, (SqlQueryExpression)expression);
		}

		public static ITable ExecuteCreateView(this IQuery query, string viewName, SqlQueryExpression queryExpression) {
			return ExecuteCreateView(query, viewName, new string[0], queryExpression);
		}

		public static ITable ExecuteCreateView(this IQuery query, string viewName, IEnumerable<string> columnNames,
			SqlQueryExpression queryExpression) {
			var statement = new CreateViewStatement(viewName, columnNames, queryExpression);
			return query.ExecuteStatement(statement);
		}

		#endregion
	}
}
