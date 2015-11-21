using System;

namespace Deveel.Data.Sql.Cursors {
	public static class CursorScopeExtensions {
		public static Cursor GetCursor(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.GetCursor(cursorName);
		}
	}
}
