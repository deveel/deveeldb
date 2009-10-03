// 
//  FunctionType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	/// The different type of a function.
	/// </summary>
	public enum FunctionType {
		/// <summary>
		/// A type that represents a static function.
		/// </summary>
		/// <remarks>
		/// A static function is not an aggregate therefore does not 
		/// require a <see cref="IGroupResolver"/>. The result of a 
		/// static function is guarenteed the same given identical 
		/// parameters over subsequent calls.
		/// </remarks>
		Static = 1,

		/// <summary>
		/// A type that represents an aggregate function.
		/// </summary>
		/// <remarks>
		/// An aggregate function requires the IGroupResolver variable 
		/// to be present in able to resolve the function over some set.
		/// The result of an aggregate function is guarenteed the same 
		/// given the same set and identical parameters.
		/// </remarks>
		Aggregate = 2,

		/// <summary>
		/// A function that is non-aggregate but whose return value is not 
		/// guarenteed to be the same given the identical parameters over 
		/// subsequent calls.
		/// </summary>
		/// <remarks>
		/// This would include functions such as RANDOM and UNIQUEKEY. The 
		/// result is dependant on some other state (a random seed and a 
		/// sequence value).
		/// </remarks>
		StateBased = 3
	}
}