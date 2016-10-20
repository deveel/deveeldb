using System;

using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Routines {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseRoutines(this ISystemBuilder builder) {
			builder.Use<IObjectManager>(
				options => options.To<RoutineManager>()
				.HavingKey(DbObjectType.Routine)
				.InTransactionScope());

			builder.Use<ITableCompositeSetupCallback>(options => options
				.To<RoutinesInit>()
				.InQueryScope());

			builder.Use<IDatabaseCreateCallback>(options => options
				.To<RoutinesInit>()
				.InQueryScope());

			builder.Use<IRoutineResolver>(options => options
				.To<SystemFunctionsProvider>()
				.InDatabaseScope());

			builder.Use<IRoutineResolver>(options => options
				.To<RoutineManager>()
				.InTransactionScope());

			builder.Use<ITableContainer>(options => options
				.To<RoutinesTableContainer>()
				.InTransactionScope());

			return builder;
		}
	}
}
