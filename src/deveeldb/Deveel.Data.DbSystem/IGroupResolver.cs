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

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Defines a contract used by grouping functions to find information 
	/// about the current group being evaluated.
	/// </summary>
	public interface IGroupResolver {
		/// <summary>
		/// A number that uniquely identifies this group from all the others 
		/// in the set of groups.
		/// </summary>
		int GroupId { get; }

		/// <summary>
		/// Gets the total number of items in this group.
		/// </summary>
		int Count { get; }

		/// <summary>
		/// Returns the value of a variable of a group.
		/// </summary>
		/// <param name="variable"></param>
		/// <param name="setIndex"></param>
		/// <remarks>
		/// The set index signifies the set item of the group.  For example, 
		/// if the group contains 10 items, then set_index may be between 0 
		/// and 9.  Return types must be either a String, BigDecimal or Boolean.
		/// </remarks>
		/// <returns></returns>
		DataObject Resolve(ObjectName variable, int setIndex);

		/// <summary>
		/// Returns a <see cref="IVariableResolver"/> that can be used to 
		/// resolve variable in the get set of the group.
		/// </summary>
		/// <param name="setIndex"></param>
		/// <remarks>
		/// The object returned is undefined after the next call to this method.
		/// </remarks>
		/// <returns></returns>
		IVariableResolver GetVariableResolver(int setIndex);
	}
}