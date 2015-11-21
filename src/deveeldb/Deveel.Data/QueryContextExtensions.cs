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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Query;

namespace Deveel.Data {
	public static class QueryContextExtensions {
		internal static IUserSession Session(this IQueryContext context) {
			if ((context is QueryContextBase))
				return ((QueryContextBase) context).Session;

			return null;
		}

		public static User User(this IQueryContext context) {
			return context.Session().SessionInfo.User;
		}

		public static string UserName(this IQueryContext context) {
			return context.User().Name;
		}

		internal static IQueryContext ForSystemUser(this IQueryContext queryContext) {
			return new SystemQueryContext(queryContext.Session().Transaction, queryContext.CurrentSchema);
		}

		#region Properties

		public static bool IgnoreIdentifiersCase(this IQueryContext context) {
			return context.Session().IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this IQueryContext context, bool value) {
			context.Session().IgnoreIdentifiersCase(value);
		}

		public static void AutoCommit(this IQueryContext context, bool value) {
			context.Session().AutoCommit(value);
		}

		public static bool AutoCommit(this IQueryContext context) {
			return context.Session().AutoCommit();
		}

		public static string CurrentSchema(this IQueryContext context) {
			return context.Session().CurrentSchema;
		}

		public static void CurrentSchema(this IQueryContext context, string value) {
			context.Session().CurrentSchema(value);
		}

		public static void ParameterStyle(this IQueryContext context, QueryParameterStyle value) {
			context.Session().ParameterStyle(value);
		}

		public static QueryParameterStyle ParameterStyle(this IQueryContext context) {
			return context.Session().ParameterStyle();
		}

		public static IDatabaseContext DatabaseContext(this IQueryContext context) {
			return context.Session().Database.DatabaseContext;
		}

		public static ISystemContext SystemContext(this IQueryContext context) {
			return context.DatabaseContext().SystemContext;
		}

		#endregion


		#region Tables

		public static ITableQueryInfo GetTableQueryInfo(this IQueryContext context, ObjectName tableName, ObjectName alias) {
			return context.Session().GetTableQueryInfo(tableName, alias);
		}

		#endregion

		#region Transaction Complete

		public static void Commit(this IQueryContext queryContext) {
			queryContext.Session().Commit();
		}

		public static void Rollback(this IQueryContext queryContext) {
			queryContext.Session().Rollback();
		}

		#endregion
	}
}
