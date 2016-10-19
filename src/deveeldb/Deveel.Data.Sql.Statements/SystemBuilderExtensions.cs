using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Statements {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseStatementCache<T>(this ISystemBuilder builder) where T : class, IStatementCache {
			builder.ServiceContainer.Bind<IStatementCache>()
				.To<T>()
				.InSystemScope();

			return builder;
		}

		public static ISystemBuilder UseDefaultStatementCache(this ISystemBuilder builder) {
			return builder.UseStatementCache<StatementCache>();
		}
	}
}
