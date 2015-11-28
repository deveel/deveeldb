using System;

using Deveel.Data.Routines;
using Deveel.Data.Services;

namespace Deveel.Data.Spatial {
	class SpatialModule : ISystemModule {
		public string ModuleName {
			get { return "Deveel.GIS"; }
		}

		public string Version {
			get { return typeof (SpatialModule).Assembly.GetName().Version.ToString(); }
		}

		public void Register(IScope systemScope) {
			systemScope.Register<SpatialTypeResolver>();
			systemScope.RegisterInstance<IRoutineResolver>(SpatialSystemFunctions.Resolver);
		}
	}
}
