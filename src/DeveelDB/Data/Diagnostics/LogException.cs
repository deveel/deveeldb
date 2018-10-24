using System;
using System.Runtime.Serialization;

namespace Deveel.Data.Diagnostics {
	public class LogException : SystemException {
		public LogException() {
		}

		public LogException(string message) : base(message) {
		}

		public LogException(string message, Exception innerException) : base(message, innerException) {
		}
	}
}