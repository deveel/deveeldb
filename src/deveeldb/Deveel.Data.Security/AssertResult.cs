using System;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	public sealed class AssertResult {
		private AssertResult(AccessPolicy policy, Exception error) {
			Policy = policy;
			Error = error;
		}

		public AccessPolicy Policy { get; private set; }

		public Exception Error { get; private set; }

		public bool IsAllowed {
			get { return Policy == AccessPolicy.Allow; }
		}

		public bool IsDenied {
			get { return Policy == AccessPolicy.Deny; }
		}

		public static AssertResult Allow() {
			return new AssertResult(AccessPolicy.Allow, null);
		}

		public static AssertResult Deny(Exception error) {
			return new AssertResult(AccessPolicy.Deny, error);
		}

		public static AssertResult Deny(string errorMessage) {
			return Deny(new SecurityException(errorMessage));
		}

		public static AssertResult Deny() {
			return new AssertResult(AccessPolicy.Deny, null);
		}
	}
}
