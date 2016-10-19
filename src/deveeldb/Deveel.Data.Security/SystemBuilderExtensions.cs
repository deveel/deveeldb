using System;

using Deveel.Data.Services;

namespace Deveel.Data.Security {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseSecurity(this ISystemBuilder builder) {
			builder.ServiceContainer.Register<SecurityModule>();
			return builder;
		}
	}
}
