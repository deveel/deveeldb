using System;

namespace Deveel.Data.Security {
	public class AuthenticationException : SecurityException {
		public AuthenticationException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public AuthenticationException(string message)
			: base(message) {
		}

		public AuthenticationException() {
		}
	}
}