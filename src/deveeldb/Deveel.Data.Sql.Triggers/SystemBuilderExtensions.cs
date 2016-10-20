using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Triggers {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTriggers(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<IObjectManager>()
				.To<TriggerManager>()
				.WithKey(DbObjectType.Trigger)
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeCreateCallback>()
				.To<TriggersInit>()
				.InQueryScope();

			builder.ServiceContainer.Bind<ITableContainer>()
				.To<OldAndNewTableContainer>()
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableContainer>()
				.To<TriggersTableContainer>()
				.InTransactionScope();

			return builder;
		}
	}
}