using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Sequences {
	static class ScopeExtensions {
		public static void UseSequences(this IScope scope) {
			scope.Bind<IObjectManager>()
				.To<SequenceManager>()
				.WithKey(DbObjectType.Sequence)
				.InTransactionScope();

			scope.Bind<ITableCompositeCreateCallback>()
				.To<SequencesInit>()
				.InQueryScope();

			scope.Bind<ITableContainer>()
				.To<SequenceTableContainer>()
				.InTransactionScope();
		}
	}
}
