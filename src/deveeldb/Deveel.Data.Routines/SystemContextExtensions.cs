// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data;
using Deveel.Data.Services;

namespace Deveel.Data.Routines {
	public static class SystemContextExtensions {
		public static IRoutine ResolveRoutine(this ISystemContext context, Invoke invoke, IQuery queryContext) {
			var resolvers = context.ResolveAllServices<IRoutineResolver>();
			foreach (var resolver in resolvers) {
				var routine = resolver.ResolveRoutine(invoke, queryContext);
				if (routine != null)
					return routine;
			}

			return null;
		}

		public static void UseRoutineResolver<TResolver>(this ISystemContext context) where TResolver : class, IRoutineResolver {
			context.RegisterService<TResolver>();
		}

		public static void UseRoutineResolver<TResolver>(this ISystemContext context, TResolver resolver)
			where TResolver : class, IRoutineResolver {
			context.RegisterInstance(resolver);
		}

		public static void UseSystemFunctions(this ISystemContext context) {
			context.UseRoutineResolver(new SystemFunctionsProvider());
		}
	}
}
