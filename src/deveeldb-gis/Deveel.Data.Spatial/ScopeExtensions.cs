using System;

using Deveel.Data.Services;

namespace Deveel.Data.Spatial {
	public static class ScopeExtensions {
		public static void UseSpatial(this IScope scope) {
			scope.Register<ISystemModule, SpatialModule>();
		}
	}
}
