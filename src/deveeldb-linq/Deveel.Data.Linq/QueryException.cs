using System;

namespace Deveel.Data.Linq {
	public class QueryException : Exception {
		public QueryException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public QueryException(string message)
			: base(message) {
		}

		public QueryException() {
		}
	}
}
