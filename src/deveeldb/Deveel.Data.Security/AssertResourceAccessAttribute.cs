using System;

namespace Deveel.Data.Security {
	[AttributeUsage(AttributeTargets.Class)]
	public sealed class AssertResourceAccessAttribute : SecurityAssertAttribute {
		protected override AssertResult AssertCore(ISecurityContext context) {
			if (context.User.IsSystem)
				return AssertResult.Allow();

			if (context.Target is IResourceAccess) {
				var provider = (IResourceAccess) context.Target;

				foreach (var resourceAccess in provider.Requests) {
					var resourceName = resourceAccess.Resource;
					var resourceType = resourceAccess.ResourceType;

					if (!context.User.HasPrivileges(resourceType, resourceName, resourceAccess.Privileges))
						return AssertResult.Deny(new MissingPrivilegesException(context.User.Name, resourceName, resourceAccess.Privileges));
				}
			}

			return base.AssertCore(context);
		}
	}
}
