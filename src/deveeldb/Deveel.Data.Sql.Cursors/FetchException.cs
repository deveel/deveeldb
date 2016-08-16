using System;

namespace Deveel.Data.Sql.Cursors {
	public sealed class FetchException : CursorException {
		internal FetchException(string cursorName, string message)
			: this(cursorName, message, null) {
		}

		internal FetchException(string cursorName, string message, Exception innerException)
			: base(cursorName, SystemErrorCodes.CursorFetchError, message, innerException) {
		}

		internal FetchException(string cursorName, Exception innerException)
			: this(cursorName, FormatMessage(cursorName), innerException) {
		}

		private static string FormatMessage(string cursorName) {
			return String.Format("The cursor '{0}' caused an unknown error: see inner exception for details.", cursorName);
		}
	}
}
