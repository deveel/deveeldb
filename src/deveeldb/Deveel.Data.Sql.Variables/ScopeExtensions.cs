using System;

using Deveel.Data.Caching;
using Deveel.Data.Services;

namespace Deveel.Data.Sql.Variables {
	static class ScopeExtensions {
		public static void UseVariables(this IScope scope) {
			scope.Bind<IObjectManager>()
				.To<PersistentVariableManager>()
				.WithKey(DbObjectType.Variable)
				.InTransactionScope();

			scope.Bind<ISystemCreateCallback>()
				.To<PersistentVariableManager>()
				.InTransactionScope();

			scope.Bind<ITableCellCache>()
				.To<TableCellCache>()
				.InSystemScope();
		}
	}
}