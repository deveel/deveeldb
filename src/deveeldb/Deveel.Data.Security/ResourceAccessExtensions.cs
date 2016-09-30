using System;

namespace Deveel.Data.Security {
	public static class ResourceAccessExtensions {
		public static AssertResult Assert(this IResourceAccess resourceAccess, ISecurityContext context) {
			foreach (var request in resourceAccess.AccessRequests) {
				var resourceName = request.Resource;
				var resourceType = request.ResourceType;

				if (!context.User.HasPrivileges(resourceType, resourceName, request.Privileges))
					return AssertResult.Deny(new MissingPrivilegesException(context.User.Name, resourceName, request.Privileges));
			}

			return AssertResult.Allow();
		}
	}
}
