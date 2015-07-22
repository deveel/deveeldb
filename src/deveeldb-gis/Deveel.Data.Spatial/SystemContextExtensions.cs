using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Spatial {
	public static class SystemContextExtensions {
		public static void UserSpatial(this ISystemContext context) {
			context.ServiceProvider.Register<SpatialTypeResolver>();
			context.ServiceProvider.Register(SpatialSystemFunctions.Resolver);
		}
	}
}
