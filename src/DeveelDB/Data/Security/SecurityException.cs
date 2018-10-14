using System;

namespace Deveel.Data.Security {
	public class SecurityException : SystemException {
		public SecurityException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public SecurityException(string message)
			: base(message) {
		}

		public SecurityException() {
		}
	}
}