using System;

using Deveel.Data.Routines;
using Deveel.Data.Services;

namespace Deveel.Data.Spatial {
	class SpatialModule : ISystemModule {
		public void Register(IScope systemScope) {
			systemScope.Register<SpatialTypeResolver>();
			systemScope.RegisterInstance<IRoutineResolver>(SpatialSystemFunctions.Resolver);
		}
	}
}
