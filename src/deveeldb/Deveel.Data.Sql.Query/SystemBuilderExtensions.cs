using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Query {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseQueryPlanner<T>(this ISystemBuilder builder) where T : class, IQueryPlanner {
			builder.ServiceContainer.Bind<IQueryPlanner>()
				.To<T>()
				.InSystemScope();

			return builder;
		}

		public static ISystemBuilder UseDefaultQueryPlanner(this ISystemBuilder builder) {
			return builder.UseQueryPlanner<QueryPlanner>();
		}
	}
}
