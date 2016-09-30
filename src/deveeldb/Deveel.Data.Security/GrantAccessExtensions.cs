using System;

namespace Deveel.Data.Security {
	public static class GrantAccessExtensions {
		public static void Grant(this IGrantAccess access, ISecurityContext context) {
			foreach (var request in access.GrantRequests) {
				context.Request.Access().GrantOn(request.ResourceType, request.Resource, context.User.Name, request.Privileges, true);
			}
		}
	}
}
