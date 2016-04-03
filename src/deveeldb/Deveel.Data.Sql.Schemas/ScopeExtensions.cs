using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Schemas {
	static class ScopeExtensions {
		public static void UseSchema(this IScope scope) {
			scope.Bind<IObjectManager>()
				.To<SchemaManager>()
				.WithKey(DbObjectType.Schema)
				.InTransactionScope();

			scope.Bind<ITableCompositeCreateCallback>()
				.To<SchemaInit>()
				.InQueryScope();

			scope.Bind<IDatabaseCreateCallback>()
				.To<InfortmationSchemaCreate>()
				.InTransactionScope();
		}
	}
}