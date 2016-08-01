using System;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorOutOfBoundsException : CursorException {
		internal CursorOutOfBoundsException(string cursorName)
			: base(cursorName, SystemErrorCodes.CursorOutOfBounds, FormatMessage(cursorName)) {
		}

		private static string FormatMessage(string cursorName) {
			return String.Format("The cursor '{0}' is out of bounds.", cursorName);
		}
	}
}
