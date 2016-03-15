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
	}
}
