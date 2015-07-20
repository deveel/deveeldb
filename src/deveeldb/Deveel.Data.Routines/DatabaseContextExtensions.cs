// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Routines {
	public static class DatabaseContextExtensions {
		public static void RegisterRoutineResolver(this IDatabaseContext context, IRoutineResolver resolver) {
			var container = context.RoutineResolver as IRoutineResolverContainer;
			if (container == null)
				return;

			container.RegisterResolver(resolver);
		}

		public static void UseSystemFunctions(this IDatabaseContext context) {
			context.RegisterRoutineResolver(SystemFunctions.Provider);
		}

		public static void UseSystemProcedures(this IDatabaseContext context) {
			context.RegisterRoutineResolver(SystemProcedures.Resolver);
		}

		public static IRoutine ResolveRoutine(this IDatabaseContext context, Invoke invoke, IQueryContext queryContext) {
			if (context.RoutineResolver == null)
				return null;

			return context.RoutineResolver.ResolveRoutine(invoke, queryContext);
		}

		public static RoutineInfo ResolveRoutineInfo(this IDatabaseContext context, Invoke invoke, IQueryContext queryContext) {
			var routine = context.ResolveRoutine(invoke, queryContext);
			if (routine == null)
				return null;

			return routine.RoutineInfo;
		}

		public static FunctionInfo ResolveFunctionInfo(this IDatabaseContext context, Invoke invoke, IQueryContext queryContext) {
			return context.ResolveRoutineInfo(invoke, queryContext) as FunctionInfo;
		}

		public static ProcedureInfo ResolveProcedureInfo(this IDatabaseContext context, Invoke invoke,
			IQueryContext queryContext) {
			return context.ResolveRoutineInfo(invoke, queryContext) as ProcedureInfo;
		}
	}
}
