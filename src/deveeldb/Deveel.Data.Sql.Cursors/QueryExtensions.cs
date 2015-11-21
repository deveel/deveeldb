using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Cursors {
	public static class QueryExtensions {
		public static void DeclareCursor(this IQuery query, CursorInfo cursorInfo) {
			query.QueryContext.DeclareCursor(cursorInfo);
		}

		public static bool CursorExists(this IQuery query, string cursorName) {
			return query.QueryContext.CursorExists(cursorName);
		}

		public static bool DropCursor(this IQuery query, string cursorName) {
			return query.QueryContext.DropCursor(cursorName);
		}

		public static Cursor FindCursor(this IQuery query, string cursorName) {
			return query.QueryContext.FindCursor(cursorName);
		}

		public static bool OpenCursor(this IQuery query, string cursorName, params SqlExpression[] args) {
			return query.QueryContext.OpenCursor(query, cursorName, args);
		}
	}
}
