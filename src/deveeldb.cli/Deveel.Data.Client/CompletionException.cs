using System;

namespace Deveel.Data.Client {
	public class CompletionException : Exception {
		public CompletionException(string message)
			: this(new string[] {message}, message) {
		}

		public CompletionException(string[] errors)
			: this(errors, "Error completing input") {
		}

		public CompletionException(string[] errors, string message)
			: this(errors, message, null) {
		}

		public CompletionException(string[] errors, string message, Exception innerException)
			: base(message, innerException) {
			Errors = errors;
		}

		public string[] Errors { get; private set; }
	}
}
