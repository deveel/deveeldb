using System;

namespace Deveel.Data.Diagnostics {
	public class ErrorEvent : Event {
		public ErrorEvent(Exception error, int errorCode, ErrorLevel level) {
			if (error == null)
				throw new ArgumentNullException("error");

			Error = error;
			ErrorCode = errorCode;
			Level = level;
		}

		public Exception Error { get; private set; }

		public int ErrorCode { get; private set; }

		public ErrorLevel Level { get; private set; }
	}
}
