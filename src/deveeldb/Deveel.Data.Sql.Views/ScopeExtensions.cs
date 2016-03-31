using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Views {
	public static class ScopeExtensions {
		public static void UseViews(this IScope systemScope) {
			systemScope.Register<ViewsModule>();
		}

		public static void UseViews<TModule>(this ServiceContainer systemScope)
			where TModule : class, ISystemModule {
			systemScope.Unregister<ViewsModule>();
			systemScope.Register<TModule>();
		}
	}
}