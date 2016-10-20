using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Sequences {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSequences(this ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
				.To<SequenceManager>()
				.HavingKey(DbObjectType.Sequence)
				.InTransactionScope());

			builder.Use<ITableCompositeCreateCallback>(options => options
				.To<SequencesInit>()
				.InQueryScope());

			builder.Use<ITableContainer>(optiions => optiions
				.To<SequenceTableContainer>()
				.InTransactionScope());

			return builder;
		}
	}
}