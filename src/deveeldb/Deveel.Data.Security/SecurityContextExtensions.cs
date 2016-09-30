using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Security {
	public static class SecurityContextExtensions {
		public static SecurityAssertionResult Assert(this ISecurityContext context) {
			var assertions = context.Assertions;
			var results = new List<AssertResult>();

			foreach (var assert in assertions) {
				AssertResult result;

				try {
					result = assert.Assert(context);
				} catch (Exception ex) {
					result = AssertResult.Deny(ex);
				}

				results.Add(result);
			}

			return new SecurityAssertionResult(results);
		}
	}
}
