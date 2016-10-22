using System;

using Deveel.Data.Build;
using Deveel.Data.Services;

namespace Deveel.Data.Sql.Statements {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseStatementCache<T>(this ISystemBuilder builder) where T : class, IStatementCache {
			return builder.Use<IStatementCache>(options => options.With<T>().InSystemScope());
		}

		public static ISystemBuilder UseDefaultStatementCache(this ISystemBuilder builder) {
			return builder.UseStatementCache<StatementCache>();
		}
	}
}
