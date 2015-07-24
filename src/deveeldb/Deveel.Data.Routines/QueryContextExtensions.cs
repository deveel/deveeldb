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
	public static class QueryContextExtensions {
		public static bool IsSystemFunction(this IQueryContext context, Invoke invoke) {
			var info = context.ResolveFunctionInfo(invoke);
			if (info == null)
				return false;

			return info.FunctionType != FunctionType.External &&
			       info.FunctionType != FunctionType.UserDefined;
		}

		public static IFunction ResolveFunction(this IQueryContext context, Invoke invoke) {
			return context.DatabaseContext().ResolveRoutine(invoke, context) as IFunction;
		}

		public static IFunction ResolveFunction(this IQueryContext context, ObjectName functionName, params SqlExpression[] args) {
			var invoke = new Invoke(functionName, args);
			return context.ResolveFunction(invoke);
		}

		public static FunctionInfo ResolveFunctionInfo(this IQueryContext context, Invoke invoke) {
			return context.DatabaseContext().ResolveFunctionInfo(invoke, context);
		}

		public static DataObject InvokeSystemFunction(this IQueryContext context, string functionName,
			params SqlExpression[] args) {
			var resolvedName = new ObjectName(SystemSchema.SchemaName, functionName);
			var invoke = new Invoke(resolvedName, args);
			return context.InvokeFunction(invoke);
		}

		public static DataObject InvokeFunction(this IQueryContext context, Invoke invoke) {
			var result = invoke.Execute(context);
			return result.ReturnValue;
		}

		public static DataObject InvokeFunction(this IQueryContext context, ObjectName functionName,
			params SqlExpression[] args) {
			return context.InvokeFunction(new Invoke(functionName, args));
		}
	}
}
