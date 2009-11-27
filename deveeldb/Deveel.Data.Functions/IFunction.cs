//  
//  IFunction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Functions {
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
		/// Returns the list of Variable objects that this function uses as its parameters.
		/// </summary>
		/// <remarks>
		/// If this returns an empty list, then the function must only have constant 
		/// parameters.  This information can be used to optimize evaluation because 
		/// if all the parameters of a function are constant then we only need to evaluate 
		/// the function once.
		/// </remarks>
		IList AllVariables { get; }

		/// <summary>
		/// Returns the list of all element objects that this function uses as its parameters.
		/// </summary>
		/// <remarks>
		/// If this returns an empty list, then the function has no input elements at 
		/// all. (something like: <c>upper(user())</c>)
		/// </remarks>
		IList AllElements { get; }


		/// <summary>
		/// Returns true if this function is an aggregate function.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// An aggregate function requires that the IGroupResolver is not null when 
		/// the evaluate method is called.
		/// </remarks>
		/// <returns></returns>
		bool IsAggregate(IQueryContext context);

		/// <summary>
		/// Prepares the exressions that are the parameters of this function.
		/// </summary>
		/// <param name="preparer"></param>
		/// <remarks>
		/// This is intended to be used if we need to resolve aspects such as 
		/// <see cref="VariableName"/> references. For example, a variable reference 
		/// to <i>number</i> may become <i>APP.Table.NUMBER</i>.
		/// </remarks>
		void PrepareParameters(IExpressionPreparer preparer);

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