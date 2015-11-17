using System;

using Deveel.Data;
using Deveel.Data.Routines;
using Deveel.Data.Services;

namespace Deveel.Data.Spatial {
	public static class SystemContextExtensions {
		public static void UserSpatial(this ISystemContext context) {
			context.ServiceProvider.Register<SpatialTypeResolver>();
			context.ServiceProvider.Register<IRoutineResolver>(SpatialSystemFunctions.Resolver);
		}
	}
}
