using System;

using Deveel.Data;
using Deveel.Data.Routines;
using Deveel.Data.Services;

namespace Deveel.Data.Spatial {
	public static class SystemContextExtensions {
		public static void UserSpatial(this ISystemContext context) {
			context.RegisterService<SpatialTypeResolver>();
			context.RegisterInstance<IRoutineResolver>(SpatialSystemFunctions.Resolver);
		}
	}
}
