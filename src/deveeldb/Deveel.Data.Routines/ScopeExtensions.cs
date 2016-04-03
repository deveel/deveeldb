using System;

using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Routines {
	static class ScopeExtensions {
		public static void UseRoutines(this IScope systemScope) {
			systemScope.Bind<IObjectManager>()
				.To<RoutineManager>()
				.WithKey(DbObjectType.Routine)
				.InTransactionScope();

			systemScope.Bind<ITableCompositeSetupCallback>()
				.To<RoutinesInit>()
				.InQueryScope();

			systemScope.Bind<IDatabaseCreateCallback>()
				.To<RoutinesInit>()
				.InQueryScope();

			systemScope.Bind<IRoutineResolver>()
				.To<SystemFunctionsProvider>()
				.InDatabaseScope();

			systemScope.Bind<IRoutineResolver>()
				.To<RoutineManager>()
				.InTransactionScope();

			systemScope.Bind<ITableContainer>()
				.To<RoutinesTableContainer>()
				.InTransactionScope();
		}
	}
}