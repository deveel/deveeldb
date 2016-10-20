using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Tables {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTables(this ISystemBuilder builder) {
			return builder
				.Use<IObjectManager>(options => options
					.To<TableManager>()
					.HavingKey(DbObjectType.Table)
					.InTransactionScope())
				.Use<ITableCompositeSetupCallback>(options => options
					.To<TablesInit>()
					.InTransactionScope())
				.Use<ITableCompositeCreateCallback>(options => options
					.To<TablesInit>()
					.InTransactionScope());
		}
	}
}
