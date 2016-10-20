using System;

using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Types {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTypes(this ISystemBuilder builder) {
			return builder
				.Use<IObjectManager>(options => options
					.To<TypeManager>()
					.HavingKey(DbObjectType.Type)
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.To<TypesTableContainer>()
					.InTransactionScope())
				.Use<ITableCompositeCreateCallback>(options => options
					.To<TypesInit>()
					.InTransactionScope())
				.Use<ITypeResolver>(options => options
					.To<TypeManager>()
					.InTransactionScope())
				.Use<IRoutineResolver>(options => options
					.To<TypeManager>()
					.InTransactionScope());
		}
	}
}