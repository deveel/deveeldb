using System;

using Deveel.Data.DbSystem;
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
