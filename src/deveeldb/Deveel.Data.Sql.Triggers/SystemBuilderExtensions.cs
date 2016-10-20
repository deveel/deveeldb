using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Triggers {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTriggers(this ISystemBuilder builder) {
			return builder
				.Use<IObjectManager>(options => options
					.To<TriggerManager>()
					.HavingKey(DbObjectType.Trigger)
					.InTransactionScope())
				.Use<ITableCompositeCreateCallback>(options => options
					.To<TriggersInit>()
					.InQueryScope())
				.Use<ITableContainer>(options => options
					.To<OldAndNewTableContainer>()
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.To<TriggersTableContainer>()
					.InTransactionScope());
		}
	}
}