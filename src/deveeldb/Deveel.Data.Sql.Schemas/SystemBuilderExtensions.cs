using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Schemas {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSchema(this ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
				.To<SchemaManager>()
				.HavingKey(DbObjectType.Schema)
				.InTransactionScope());

			builder.Use<ITableCompositeCreateCallback>(options => options
				.To<SchemaInit>()
				.InQueryScope());

			builder.Use<IDatabaseCreateCallback>(options => options
				.To<InfortmationSchemaCreate>()
				.InTransactionScope());

			return builder;
		}
	}
}