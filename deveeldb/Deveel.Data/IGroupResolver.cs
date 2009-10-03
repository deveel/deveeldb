// 
//  IGroupResolver.cs
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

namespace Deveel.Data {
	/// <summary>
	/// Similar to <see cref="IVariableResolver"/>, this method is used by 
	/// grouping functions to find information about the current group being 
	/// evaluated (used for evaluating aggregate functions).
	/// </summary>
	public interface IGroupResolver {
		/// <summary>
		/// A number that uniquely identifies this group from all the others 
		/// in the set of groups.
		/// </summary>
		int GroupId { get; }

		/// <summary>
		/// The total number of set items in this group.
		/// </summary>
		int Count { get; }

		/// <summary>
		/// Returns the value of a variable of a group.
		/// </summary>
		/// <param name="variable"></param>
		/// <param name="set_index"></param>
		/// <remarks>
		/// The set index signifies the set item of the group.  For example, 
		/// if the group contains 10 items, then set_index may be between 0 
		/// and 9.  Return types must be either a String, BigDecimal or Boolean.
		/// </remarks>
		/// <returns></returns>
		TObject Resolve(Variable variable, int set_index);

		/// <summary>
		/// Returns a <see cref="IVariableResolver"/> that can be used to 
		/// resolve variable in the get set of the group.
		/// </summary>
		/// <param name="set_index"></param>
		/// <remarks>
		/// The object returned is undefined after the next call to this method.
		/// </remarks>
		/// <returns></returns>
		IVariableResolver GetVariableResolver(int set_index);

	}
}