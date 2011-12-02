using System;

namespace Deveel.Data.Client {
	public sealed class DeveelDbServerException : DeveelDbException {
		internal DeveelDbServerException(string message, string stackTrace, int code)
			: base(message, code) {
			this.stackTrace = stackTrace;
		}

		private readonly string stackTrace;

		public override string StackTrace {
			get { return stackTrace; }
		}
	}
}