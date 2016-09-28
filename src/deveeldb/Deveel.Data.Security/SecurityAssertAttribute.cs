using System;
using System.Linq;

namespace Deveel.Data.Security {
	[AttributeUsage(AttributeTargets.Class)]
	public class SecurityAssertAttribute : Attribute, ISecurityAssert {
		public string[] Roles { get; set; }

		protected virtual AssertResult Assert(ISecurityContext context) {
			var result = AssertIsInRole(context.User);
			if (!result.IsAllowed)
				return result;

			return AssertCore(context);
		}

		protected virtual AssertResult AssertCore(ISecurityContext context) {
			return AssertResult.Allow();
		}

		protected virtual AssertResult AssertIsInRole(User user) {
			if (Roles != null) {
				bool authorize = true;
				foreach (var role in Roles) {
					if (!user.IsInRole(role)) {
						authorize = false;
						break;
					}
				}

				if (!authorize)
					return AssertResult.Deny(String.Format("User '{0}' has none of the required roles ('{1}').",
						user.Name, String.Join(", ", Roles)));
			}

			return AssertResult.Allow();
		}

		AssertResult ISecurityAssert.Assert(ISecurityContext context) {
			return Assert(context);
		}
	}
}
