using System;

using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Routines {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseRoutines(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<IObjectManager>()
				.To<RoutineManager>()
				.WithKey(DbObjectType.Routine)
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeSetupCallback>()
				.To<RoutinesInit>()
				.InQueryScope();

			builder.ServiceContainer.Bind<IDatabaseCreateCallback>()
				.To<RoutinesInit>()
				.InQueryScope();

			builder.ServiceContainer.Bind<IRoutineResolver>()
				.To<SystemFunctionsProvider>()
				.InDatabaseScope();

			builder.ServiceContainer.Bind<IRoutineResolver>()
				.To<RoutineManager>()
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableContainer>()
				.To<RoutinesTableContainer>()
				.InTransactionScope();

			return builder;
		}
	}
}
