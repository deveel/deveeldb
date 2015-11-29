using System;
using System.Collections.Generic;

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

		protected override void GetEventData(Dictionary<string, object> data) {
			data["error.code"] = ErrorCode;
			data["error.level"] = Level.ToString().ToLowerInvariant();
			data["error.message"] = Error.Message;
			data["error.stackTrace"] = Error.StackTrace;
		}
	}
}
