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
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Extension methods to any <see cref="IRoutineResolver"/>
	/// </summary>
	public static class RoutineResolverExtensions {
		/// <summary>
		/// Checks if a function matched against the given request represents
		/// an aggregate function.
		/// </summary>
		/// <param name="resolver">The routine resolver.</param>
		/// <param name="request">The invocation request used to resolve the function.</param>
		/// <param name="context">The parent query context.</param>
		/// <returns>
		/// Returns <c>true</c> if a routine was resolved for the given request,
		/// this is a <see cref="IFunction"/> and the <see cref="FunctionType"/> is
		/// <see cref="FunctionType.Aggregate"/>, otherwise <c>false</c>.
		/// </returns>
		public static bool IsAggregateFunction(this IRoutineResolver resolver, Invoke request, IQueryContext context) {
			var routine = resolver.ResolveRoutine(request, context);

			var function = routine as IFunction;
			if (function == null)
				return false;

			return function.FunctionType == FunctionType.Aggregate;
		}

		public static IFunction ResolveFunction(this FunctionProvider resolver, string name) {
			return ResolveFunction(resolver, null, name);
		}

		public static IFunction ResolveFunction(this FunctionProvider resolver, IQueryContext context, string name) {
			return ResolveFunction(resolver, context, name, new SqlExpression[0]);
		}

		public static IFunction ResolveFunction(this FunctionProvider resolver, string name, params SqlExpression[] args) {
			return ResolveFunction(resolver, null, name, args);
		}

		public static IFunction ResolveFunction(this FunctionProvider resolver, string name, params DataObject[] args) {
			return ResolveFunction(resolver, null, name, args);
		}

		public static IFunction ResolveFunction(this FunctionProvider resolver, IQueryContext context, string name, params DataObject[] args) {
			var exps = new SqlExpression[0];
			if (args != null && args.Length > 0) {
				exps = new SqlExpression[args.Length];
				for (int i = 0; i < args.Length; i++) {
					exps[i] = SqlExpression.Constant(args[i]);
				}
			}

			return resolver.ResolveFunction(context, name, exps);
		}

		public static IFunction ResolveFunction(this FunctionProvider resolver, IQueryContext context, string name, SqlExpression[] args) {
			var funName = new ObjectName(new ObjectName(resolver.SchemaName), name);
			var routine = ((IRoutineResolver) resolver).ResolveRoutine(new Invoke(funName, args), context);
			if (!(routine is IFunction))
				throw new InvalidOperationException();

			return routine as IFunction;
		}
	}
}
