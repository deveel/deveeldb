using System;

namespace Deveel.Data {
	public sealed class DeclaredException {
		internal DeclaredException(int errorCode, string exceptionName) {
			ErrorCode = errorCode;
			ExceptionName = exceptionName;
		}

		public int ErrorCode { get; private set; }

		public string ExceptionName { get; private set; }
	}
}
