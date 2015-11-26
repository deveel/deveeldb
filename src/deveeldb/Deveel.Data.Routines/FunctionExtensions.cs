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

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Extension methods to any <see cref="IFunction"/>.
	/// </summary>
	public static class FunctionExtensions {
		/// <summary>
		/// Executes the function given the cintext provided.
		/// </summary>
		/// <param name="function">The function to execute.</param>
		/// <param name="request">The invocation information that was used to resolve 
		/// the function.</param>
		/// <param name="group"></param>
		/// <param name="resolver"></param>
		/// <param name="query"></param>
		/// <returns></returns>
		public static DataObject Execute(this IFunction function, Invoke request, IGroupResolver group,
			IVariableResolver resolver,
			IRequest query) {
			var execContext = new InvokeContext(request, function, resolver, group, query);
			var result = function.Execute(execContext);
			return result.ReturnValue;
		}

		public static SqlType ReturnType(this IFunction function, Invoke request, IRequest query, IVariableResolver resolver) {
			var execContext = new InvokeContext(request, function, resolver, null, query);
			return function.ReturnType(execContext);
		}
	}
}