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
using System.Collections;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Represents a function that is part of an expression to be evaluated.
	/// </summary>
	/// <remarks>
	/// A function evaluates to a resultant Object.  If the parameters of a function
	/// are not constant values, then the evaluation will require a lookup via a
	/// <see cref="IVariableResolver"/> or <see cref="IGroupResolver"/>. The 
	/// <see cref="IGroupResolver"/> helps evaluate an aggregate function.
	/// </remarks>
	public interface IFunction {
		/// <summary>
		/// Returns the name of the function.
		/// </summary>
		/// <remarks>
		/// The name is a unique identifier that can be used to recreate this function. 
		/// This identifier can be used to easily serialize the function when grouped 
		/// with its parameters.
		/// </remarks>
		string Name { get; }


		/// <summary>
		/// Evaluates the function and returns a TObject that represents the result of the function.
		/// </summary>
		/// <param name="group"></param>
		/// <param name="resolver"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// The <see cref="IVariableResolver"/> object should be used to look up variables in the parameter 
		/// of the function. The  <see cref="FunctionTable"/> object should only be used when the function is 
		/// a grouping function. For example, <c>avg(value_of)</c>.
		/// </remarks>
		/// <returns></returns>
		TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context);

		/// <summary>
		/// The type of object this function returns. 
		/// </summary>
		/// <param name="resolver"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// The <see cref="IVariableResolver"/> points to a dummy row that can be used to dynamically 
		/// determine the return type. For example, an implementation of SQL 'GREATEST' would return 
		/// the same type as the list elements.
		/// </remarks>
		/// <returns></returns>
		TType ReturnTType(IVariableResolver resolver, IQueryContext context);

	}
}