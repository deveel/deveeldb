//  
//  IVariableResolver.cs
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
		TObject Resolve(Variable variable);

		/// <summary>
		/// Returns the <see cref="TType"/> of object the given variable is.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		TType ReturnTType(Variable variable);

	}
}