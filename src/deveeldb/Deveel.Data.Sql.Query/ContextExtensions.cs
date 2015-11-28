using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Query {
	public static class ContextExtensions {
		public static void UseDefaultQueryPlanner(this ISystemContext context) {
			context.UnregisterService<IQueryPlanner>();
			context.RegisterService<QueryPlanner>();
		}

		public static IQueryPlanner QueryPlanner(this IContext context) {
			return context.ResolveService<IQueryPlanner>();
		}
	}
}
