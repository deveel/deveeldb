// 
//  Copyright 2010  Deveel
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

using System;

namespace Deveel.Data.Functions {
	/// <summary>
	/// An interface that resolves and generates a <see cref="IFunction"/> 
	/// objects given a <see cref="FunctionDef"/> object.
	/// </summary>
	public interface IFunctionLookup {
		/// <summary>
		/// Generate the <see cref="IFunction"/> given a <see cref="FunctionDef"/> object.
		/// </summary>
		/// <param name="function_def"></param>
		/// <returns></returns>
		/// <remarks>
		/// Returns null if the <see cref="FunctionDef"/> can not be resolved 
		/// to a valid function object.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the specification of the function is invalid for some reason (the number 
		/// or type of the parameters is incorrect).
		/// </exception>
		IFunction GenerateFunction(FunctionDef function_def);

		/// <summary>
		/// Checks if the given function is aggregate.
		/// </summary>
		/// <param name="function_def"></param>
		/// <returns>
		/// Returns true if the function defined by <see cref="FunctionDef"/> is 
		/// an aggregate function, or false otherwise.
		/// </returns>
		bool IsAggregate(FunctionDef function_def);

	}
}