using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Sequences {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSequences(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<IObjectManager>()
				.To<SequenceManager>()
				.WithKey(DbObjectType.Sequence)
				.InTransactionScope();

			builder.ServiceContainer.Bind<ITableCompositeCreateCallback>()
				.To<SequencesInit>()
				.InQueryScope();

			builder.ServiceContainer.Bind<ITableContainer>()
				.To<SequenceTableContainer>()
				.InTransactionScope();

			return builder;
		}
	}
}