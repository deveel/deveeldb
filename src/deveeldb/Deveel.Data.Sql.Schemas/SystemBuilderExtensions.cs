using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Schemas {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSchema(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<IObjectManager>()
				.To<SchemaManager>()
				.WithKey(DbObjectType.Schema)
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeCreateCallback>()
				.To<SchemaInit>()
				.InQueryScope();

			builder.ServiceContainer.Bind<IDatabaseCreateCallback>()
				.To<InfortmationSchemaCreate>()
				.InTransactionScope();

			return builder;
		}
	}
}