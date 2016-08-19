using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Cursors {
	public class CursorException : SqlErrorException {
		internal CursorException(string cursorName, int errorCode, string message)
			: this(cursorName, errorCode, message, null) {
		}

		internal CursorException(string cursorName, int errorCode, string message, Exception innerException)
			: base(errorCode, message, innerException) {
			CursorName = cursorName;
		}

		internal CursorException(string cursorName, Exception innerException)
			: this(cursorName, SystemErrorCodes.CursorGeneralError, FormatMessage(cursorName), innerException) {
		}

		public string CursorName { get; private set; }

		protected override void GetMetadata(IDictionary<string, object> metadata) {
			metadata["cursor.name"] = CursorName;
		}

		private static string FormatMessage(string cursorName) {
			return String.Format("The cursor '{0}' caused an unknown error: see inner exception for details.", cursorName);
		}
	}
}
