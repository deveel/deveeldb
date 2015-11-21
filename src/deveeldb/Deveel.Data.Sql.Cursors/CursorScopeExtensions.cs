using System;

namespace Deveel.Data.Sql.Cursors {
	public static class CursorScopeExtensions {
		public static void DeclareCursor(this ICursorScope scope, CursorInfo cursorInfo) {
			scope.CursorManager.DeclareCursor(cursorInfo);
		}

		public static Cursor GetCursor(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.GetCursor(cursorName);
		}

		public static bool CursorExists(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.CursorExists(cursorName);
		}

		public static bool DropCursor(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.DropCursor(cursorName);
		}
	}
}
