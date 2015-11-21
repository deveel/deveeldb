using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Query {
	public static class ContextExtensions {
		public static void UseDefaultQueryPlanner(this ISystemContext context) {
			context.UnregisterService<IQueryPlanner>();
			context.RegisterService<QueryPlanner>();

			QueryPlanSerializers.RegisterSerializers(context);
		}

		public static IQueryPlanner QueryPlanner(this IContext context) {
			return context.ResolveService<IQueryPlanner>();
		}
	}
}
