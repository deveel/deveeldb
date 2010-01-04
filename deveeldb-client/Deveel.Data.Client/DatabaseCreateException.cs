using System;

namespace Deveel.Data.Client {
	public sealed class DatabaseCreateException : DeveelDbException {
		internal DatabaseCreateException(string message)
			: base(message) {
		}
	}
}