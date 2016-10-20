using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Tables {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTables(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<IObjectManager>()
				.To<TableManager>()
				.WithKey(DbObjectType.Table)
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeSetupCallback>()
				.To<TablesInit>()
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeCreateCallback>()
				.To<TablesInit>()
				.InTransactionScope();

			return builder;
		}
	}
}
