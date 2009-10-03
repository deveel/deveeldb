// 
//  IFunctionLookup.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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