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

using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An interface to resolve a variable name to a constant object.
	/// </summary>
	/// <remarks>
	/// This is used as a way to resolve a variable into a value to use 
	/// in an expression.
	/// </remarks>
	public interface IVariableResolver {
		/// <summary>
		/// Returns the value of a given variable.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		Variable Resolve(ObjectName variable);

		/// <summary>
		/// Returns the <see cref="SqlType"/> of object the given variable is.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		SqlType ReturnType(ObjectName variable);
	}
}