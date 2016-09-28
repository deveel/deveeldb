using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Security {
	public sealed class SecurityAssertionResult {
		private readonly IEnumerable<AssertResult> results;

		internal SecurityAssertionResult(IEnumerable<AssertResult> results) {
			this.results = new List<AssertResult>(results);
		}

		public bool IsAllowed {
			get { return !results.Any() || results.All(x => x.IsAllowed); }
		}

		public bool IsDenied {
			get { return results.Any(x => x.IsDenied); }
		}

		public Exception[] Errors {
			get { return results.Where(x => x.IsDenied && x.Error != null).Select(x => x.Error).ToArray(); }
		}

		public SecurityException SecurityError {
			get {
				var error = Errors.OfType<SecurityException>().FirstOrDefault();
				if (error == null) {
					var innerError = Errors.FirstOrDefault();
					error = new SecurityException("An error occurred while performing a security assertion", innerError);
				}

				return error;
			}
		}
	}
}
