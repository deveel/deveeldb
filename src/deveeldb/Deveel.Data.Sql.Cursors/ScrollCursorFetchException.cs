using System;

namespace Deveel.Data.Sql.Cursors {
	public sealed class ScrollCursorFetchException : CursorException {
		internal ScrollCursorFetchException(string cursorName) 
			: base(cursorName, SystemErrorCodes.ScrollCursorFetch, FormatMessage(cursorName)) {
		}

		private static string FormatMessage(string cursorName) {
			return String.Format("Cursor '{0}' is not SCROLL: can fetch only NEXT value.", cursorName);
		}
	}
}
