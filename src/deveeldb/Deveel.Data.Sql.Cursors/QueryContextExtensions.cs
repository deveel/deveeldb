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

using Deveel.Data;
using Deveel.Data.Security;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Cursors {
	public static class QueryContextExtensions {
		public static void DeclareCursor(this IQueryContext context, CursorInfo cursorInfo) {
			var queryPlan = context.DatabaseContext().QueryPlanner().PlanQuery(context, cursorInfo.QueryExpression, null);
			var selectedTables = queryPlan.DiscoverTableNames();
			foreach (var tableName in selectedTables) {
				if (!context.UserCanSelectFromTable(tableName))
					throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Select);
			}

			context.CursorManager.DeclareCursor(cursorInfo);
		}

		public static void CloseCursor(this IQueryContext context, string cursorName) {
			var cursor = context.FindCursor(cursorName);
			if (cursor == null)
				throw new ObjectNotFoundException(new ObjectName(cursorName));

			cursor.Close();
		}

		public static Cursor FindCursor(this IQueryContext context, string cursorName) {
			IQueryContext currentContext = context;
			while (currentContext != null) {
				var cursor = currentContext.CursorManager.GetCursor(cursorName);
				if (cursor != null)
					return cursor;

				currentContext = currentContext.ParentContext;
			}

			return null;
		}
	}
}
