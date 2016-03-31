using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Types {
	static class ScopeExtensions {
		public static void UseTypes(this IScope scope) {
			scope.Bind<IObjectManager>()
				.To<TypeManager>()
				.WithKey(DbObjectType.Type)
				.InTransactionScope();

			scope.Bind<ISystemCreateCallback>()
				.To<TypeManager>()
				.InTransactionScope();
		}
	}
}