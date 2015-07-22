using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Protocol {
	public class ProtocolException : ErrorException {
		public ProtocolException() 
			: this(-3000) {
		}

		public ProtocolException(int errorCode) 
			: base(EventClasses.Protocol, errorCode) {
		}

		public ProtocolException(string message) 
			: this(-3000, message) {
		}

		public ProtocolException(int errorCode, string message) 
			: base(EventClasses.Protocol, errorCode, message) {
		}

		public ProtocolException(string message, Exception innerException) 
			: this(-3000, message, innerException) {
		}

		public ProtocolException(int errorCode, string message, Exception innerException) 
			: base(EventClasses.Protocol, errorCode, message, innerException) {
		}
	}
}
