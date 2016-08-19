using System;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorOutOfContextException : CursorException {
		internal CursorOutOfContextException(string cursorName)
			: base(cursorName, SystemErrorCodes.CursorOutOfContext, FormatMessage(cursorName)) {
		}

		private static string FormatMessage(string cursorName) {
			return String.Format("The sensitive cursor '{0}' requires an active context.", cursorName);
		}
	}
}
