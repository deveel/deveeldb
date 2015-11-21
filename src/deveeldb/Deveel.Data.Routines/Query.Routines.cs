using System;

using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	public static class Query {
		public static bool IsSystemFunction(this IQuery context, Invoke invoke) {
			var info = context.ResolveFunctionInfo(invoke);
			if (info == null)
				return false;

			return info.FunctionType != FunctionType.External &&
				   info.FunctionType != FunctionType.UserDefined;
		}

		public static bool IsAggregateFunction(this IQuery context, Invoke invoke) {
			var function = context.ResolveFunction(invoke);
			return function != null && function.FunctionType == FunctionType.Aggregate;
		}

		public static IRoutine ResolveRoutine(this IQuery context, Invoke invoke) {
			var routine = context.ResolveSystemRoutine(invoke);
			if (routine == null)
				routine = context.ResolveUserRoutine(invoke);

			return routine;
		}

		public static IRoutine ResolveSystemRoutine(this IQuery context, Invoke invoke) {
			// return context.SystemContext().ResolveRoutine(invoke, context);

			var resolvers = context.QueryContext.ResolveAllServices<IRoutineResolver>();
			foreach (var resolver in resolvers) {
				var routine = resolver.ResolveRoutine(invoke, context.QueryContext);
				if (routine != null)
					return routine;
			}

			return null;
		}

		public static IRoutine ResolveUserRoutine(this IQuery context, Invoke invoke) {
			var routine = context.Session.ResolveRoutine(invoke);
			if (routine != null &&
				!context.UserCanExecute(routine.Type, invoke))
				throw new InvalidOperationException();

			return routine;
		}

		public static IFunction ResolveFunction(this IQuery context, Invoke invoke) {
			return context.ResolveRoutine(invoke) as IFunction;
		}

		public static IFunction ResolveFunction(this IQuery context, ObjectName functionName, params SqlExpression[] args) {
			var invoke = new Invoke(functionName, args);
			return context.ResolveFunction(invoke);
		}

		public static FunctionInfo ResolveFunctionInfo(this IQuery context, Invoke invoke) {
			return context.ResolveRoutineInfo(invoke) as FunctionInfo;
		}

		public static RoutineInfo ResolveRoutineInfo(this IQuery context, Invoke invoke) {
			var routine = context.ResolveRoutine(invoke);
			if (routine == null)
				return null;

			return routine.RoutineInfo;
		}

		public static DataObject InvokeSystemFunction(this IQuery context, string functionName,
			params SqlExpression[] args) {
			var resolvedName = new ObjectName(SystemSchema.SchemaName, functionName);
			var invoke = new Invoke(resolvedName, args);
			return context.InvokeFunction(invoke);
		}

		public static DataObject InvokeFunction(this IQuery context, Invoke invoke) {
			var result = invoke.Execute(context.QueryContext);
			return result.ReturnValue;
		}

		public static DataObject InvokeFunction(this IQuery context, ObjectName functionName,
			params SqlExpression[] args) {
			return context.InvokeFunction(new Invoke(functionName, args));
		}
	}
}
