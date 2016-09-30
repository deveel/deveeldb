using System;

namespace Deveel.Data.Security {
	[AttributeUsage(AttributeTargets.Class)]
	public sealed class AssertResourceAccessAttribute : SecurityAssertAttribute {
		protected override AssertResult AssertCore(ISecurityContext context) {
			if (context.User.IsSystem)
				return AssertResult.Allow();

			if (context.Target is IResourceAccess) {
				var resourceAccess = (IResourceAccess) context.Target;
				return resourceAccess.Assert(context);
			}

			return base.AssertCore(context);
		}
	}
}
