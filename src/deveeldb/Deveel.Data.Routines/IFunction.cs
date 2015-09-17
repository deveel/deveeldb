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

using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Defines a routine that is a function, that means it returns
	/// a value after its execution.
	/// </summary>
	public interface IFunction : IRoutine {
		/// <summary>
		/// Gets the type of function.
		/// </summary>
		FunctionType FunctionType { get; }

		/// <summary>
		/// Resolves the type of the returned value of the function.
		/// </summary>
		/// <param name="context">The query context used to resolve
		/// the returned type.</param>
		/// <returns>
		/// Returns a <see cref="SqlType"/> that is the type of the
		/// value returned by the function.
		/// </returns>
		SqlType ReturnType(ExecuteContext context);
	}
}