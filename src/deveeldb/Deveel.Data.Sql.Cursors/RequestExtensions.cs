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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Cursors {
	public static class RequestExtensions {
		public static void DeclareCursor(this IRequest context, CursorInfo cursorInfo) {
			var queryPlan = context.Context.QueryPlanner().PlanQuery(new QueryInfo(context, cursorInfo.QueryExpression));
			var selectedTables = queryPlan.DiscoverTableNames();
			foreach (var tableName in selectedTables) {
				if (!context.Query.IsolatedAccess.SystemAccess.UserCanSelectFromTable(tableName))
					throw new MissingPrivilegesException(context.Query.UserName(), tableName, Privileges.Select);
			}

			context.Context.DeclareCursor(cursorInfo);
		}

		public static void DeclareCursor(this IRequest context, string cursorName, SqlQueryExpression query) {
			DeclareCursor(context, cursorName, (CursorFlags)0, query);
		}

		public static void DeclareCursor(this IRequest context, string cursorName, CursorFlags flags, SqlQueryExpression query) {
			context.DeclareCursor(new CursorInfo(cursorName, flags, query));
		}

		public static void DeclareInsensitiveCursor(this IRequest context, string cursorName, SqlQueryExpression query) {
			DeclareInsensitiveCursor(context, cursorName, query, false);
		}

		public static void DeclareInsensitiveCursor(this IRequest context, string cursorName, SqlQueryExpression query, bool withScroll) {
			var flags = CursorFlags.Insensitive;
			if (withScroll)
				flags |= CursorFlags.Scroll;

			context.DeclareCursor(cursorName, flags, query);
		}

		public static bool CursorExists(this IRequest query, string cursorName) {
			return query.Context.CursorExists(cursorName);
		}

		public static bool DropCursor(this IRequest query, string cursorName) {
			return query.Context.DropCursor(cursorName);
		}

		public static Cursor FindCursor(this IRequest query, string cursorName) {
			return query.Context.FindCursor(cursorName);
		}

		public static bool OpenCursor(this IRequest query, string cursorName, params SqlExpression[] args) {
			return query.Context.OpenCursor(query, cursorName, args);
		}

		public static bool CloseCursor(this IRequest query, string cursorName) {
			return query.Context.CloseCursor(query, cursorName);
		}

	}
}
