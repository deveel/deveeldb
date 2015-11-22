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
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public static class QueryContextExtensions {
		#region Properties

		public static bool IgnoreIdentifiersCase(this IQueryContext context) {
			return context.SessionContext.TransactionContext.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this IQueryContext context, bool value) {
			context.SessionContext.TransactionContext.IgnoreIdentifiersCase(value);
		}

		public static void AutoCommit(this IQueryContext context, bool value) {
			context.SessionContext.TransactionContext.AutoCommit(value);
		}

		public static bool AutoCommit(this IQueryContext context) {
			return context.SessionContext.TransactionContext.AutoCommit();
		}

		public static string CurrentSchema(this IQueryContext context) {
			return context.SessionContext.TransactionContext.CurrentSchema();
		}

		public static void CurrentSchema(this IQueryContext context, string value) {
			context.SessionContext.TransactionContext.CurrentSchema(value);
		}

		public static void ParameterStyle(this IQueryContext context, QueryParameterStyle value) {
			context.SessionContext.TransactionContext.ParameterStyle(value);
		}

		public static QueryParameterStyle ParameterStyle(this IQueryContext context) {
			return context.SessionContext.TransactionContext.ParameterStyle();
		}

		public static ISystemContext SystemContext(this IQueryContext context) {
			return context.SessionContext.TransactionContext.DatabaseContext.SystemContext;
		}

		#endregion
	}
}
