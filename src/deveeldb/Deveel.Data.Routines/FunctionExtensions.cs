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
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public static class FunctionExtensions {
		public static DataObject Execute(this IFunction function,
			RoutineInvoke invoke,
			IGroupResolver group,
			IVariableResolver resolver,
			IQueryContext context) {
			var execContext = new ExecuteContext(invoke, function, resolver, group, context);
			var result = function.Execute(execContext);
			return result.ReturnValue;
		}

		public static DataType ReturnType(this IFunction function, RoutineInvoke invoke, IQueryContext context, IVariableResolver resolver) {
			var execContext = new ExecuteContext(invoke, function, resolver, null, context);
			return function.ReturnType(execContext);
		}
	}
}