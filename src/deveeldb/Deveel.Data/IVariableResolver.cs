// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Types;

namespace Deveel.Data {
	/// <summary>
	/// An interface to resolve a variable name to a constant object.
	/// </summary>
	/// <remarks>
	/// This is used as a way to resolve a variable into a value to use 
	/// in an expression.
	/// </remarks>
	public interface IVariableResolver {
		/// <summary>
		/// A number that uniquely identifies the current state of the variable
		/// resolver.
		/// </summary>
		/// <remarks>
		/// This typically returns the row index of the table we are resolving 
		/// variables on.
		/// </remarks>
		int SetId { get; }

		/// <summary>
		/// Returns the value of a given variable.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		TObject Resolve(VariableName variable);

		/// <summary>
		/// Returns the <see cref="TType"/> of object the given variable is.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		TType ReturnTType(VariableName variable);

	}
}