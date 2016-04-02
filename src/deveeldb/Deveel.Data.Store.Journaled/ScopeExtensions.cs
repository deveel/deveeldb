using System;

using Deveel.Data.Services;

namespace Deveel.Data.Store.Journaled {
	public static class ScopeExtensions {
		public static void UseJournaledStore(this IScope scope) {
			scope.Bind<IStoreSystem>()
				.To<JournaledStoreSystem>()
				.WithKey(DefaultStorageSystemNames.Journaled)
				.InDatabaseScope();
		}
	}
}
