using System;

using Deveel.Data.Build;

namespace Deveel.Data.Sql.Query {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseQueryPlanner<T>(this ISystemBuilder builder) where T : class, IQueryPlanner {
 			return builder.Use<IQueryPlanner>(options => options.With<T>().InSystemScope());
		}

		public static ISystemBuilder UseDefaultQueryPlanner(this ISystemBuilder builder) {
			return builder.UseQueryPlanner<QueryPlanner>();
		}
	}
}
