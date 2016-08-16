using System;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorOpenException : CursorException {
		internal CursorOpenException(string cursorName)
			: base(cursorName, SystemErrorCodes.CursorOpen, FormatMessage(cursorName)) {
		}

		private static string FormatMessage(string cursorName) {
			return String.Format("The cursor '{0} is already open.", cursorName);
		}
	}
}
