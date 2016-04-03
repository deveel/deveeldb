using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Triggers {
	static class ScopeExtensions {
		public static void UseTriggers(this IScope scope) {
			scope.Bind<IObjectManager>()
				.To<TriggerManager>()
				.WithKey(DbObjectType.Trigger)
				.InTransactionScope();

			scope.Bind<ITableCompositeCreateCallback>()
				.To<TriggersInit>()
				.InQueryScope();

			scope.Bind<ITableContainer>()
				.To<OldAndNewTableContainer>()
				.InTransactionScope();

			scope.Bind<ITableContainer>()
				.To<TriggersTableContainer>()
				.InTransactionScope();
		}
	}
}