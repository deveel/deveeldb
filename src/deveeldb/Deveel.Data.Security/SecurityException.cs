using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Security {
	[Serializable]
	public class SecurityException : ErrorException {
		public SecurityException() 
			: this(SecurityErrorCodes.Unknown) {
		}

		public SecurityException(int errorCode) 
			: this(errorCode, null) {
		}

		public SecurityException(string message) 
			: this(SecurityErrorCodes.Unknown, message) {
		}

		public SecurityException(int errorCode, string message) 
			: this(errorCode, message, null) {
		}

		public SecurityException(string message, Exception innerException) 
			: this(SecurityErrorCodes.Unknown, message, innerException) {
		}

		public SecurityException(int errorCode, string message, Exception innerException) 
			: base(EventClasses.System, errorCode, message, innerException) {
		}
	}
}
