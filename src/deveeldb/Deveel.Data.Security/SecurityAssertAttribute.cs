using System;
using System.Linq;

namespace Deveel.Data.Security {
	public class SecurityAssertAttribute : ISecurityAssert {
		public string[] Roles { get; set; }

		protected virtual AssertResult Assert(ISecurityContext context) {
			if (Roles != null) {
				bool authorize = true;
				foreach (var role in Roles) {
					if (!context.User.IsInRole(role)) {
						authorize = false;
						break;
					}
				}

				if (!authorize)
					return AssertResult.Deny(String.Format("User '{0}' has none of the required roles ('{1}').", 
						context.User.Name, String.Join(", ", Roles)));
			}

			return AssertResult.Allow();
		}
		AssertResult ISecurityAssert.Assert(ISecurityContext context) {
			return Assert(context);
		}
	}
}
