using System;

using Deveel.Data.Services;
using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	static class ScopeExtensions {
		public static void UseRoutines(this IScope systemScope) {
			systemScope.Bind<IObjectManager>()
				.To<RoutineManager>()
				.WithKey(DbObjectType.Routine)
				.InTransactionScope();

			systemScope.Bind<ISystemCreateCallback>()
				.To<RoutineManager>()
				.InTransactionScope();

			systemScope.Bind<IRoutineResolver>()
				.To<SystemFunctionsProvider>()
				.InDatabaseScope();

			systemScope.Bind<IRoutineResolver>()
				.To<RoutineManager>()
				.InTransactionScope();
		}
	}
}