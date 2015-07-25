using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Routines {
	public static class SystemContextExtensions {
		public static IRoutine ResolveRoutine(this ISystemContext context, Invoke invoke, IQueryContext queryContext) {
			var resolvers = context.ResolveServices<IRoutineResolver>();
			foreach (var resolver in resolvers) {
				var routine = resolver.ResolveRoutine(invoke, queryContext);
				if (routine != null)
					return routine;
			}

			return null;
		}

		public static void UseRoutineResolver<TResolver>(this ISystemContext context) where TResolver : IRoutineResolver {
			context.RegisterService<TResolver>();
		}

		public static void UseRoutineResolver<TResolver>(this ISystemContext context, TResolver resolver)
			where TResolver : IRoutineResolver {
			context.RegisterService(resolver);
		}

		public static void UseSystemFunctions(this ISystemContext context) {
			context.UseRoutineResolver(SystemFunctions.Provider);
		}
	}
}
