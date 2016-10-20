using System;

using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Types {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTypes(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<IObjectManager>()
				.To<TypeManager>()
				.WithKey(DbObjectType.Type)
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableContainer>()
				.To<TypesTableContainer>()
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeCreateCallback>()
				.To<TypesInit>()
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITypeResolver>()
				.To<TypeManager>()
				.InTransactionScope();

			builder.ServiceContainer.Bind<IRoutineResolver>()
				.To<TypeManager>()
				.InTransactionScope();

			return builder;
		}
	}
}