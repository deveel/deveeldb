using System;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorClosedException : CursorException {
		internal CursorClosedException(string cursorName)
			: base(cursorName, SystemErrorCodes.CursorClosed, FormatMessage(cursorName)) {
		}

		public static string FormatMessage(string cursorName) {
			return String.Format("The cursor '{0}' is closed.", cursorName);
		}
	}
}
