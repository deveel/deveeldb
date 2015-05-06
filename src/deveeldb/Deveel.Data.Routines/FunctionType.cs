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

using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
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
		/// static function is guaranteed the same given identical 
		/// parameters over subsequent calls.
		/// </remarks>
		Static = 1,

		/// <summary>
		/// A type that represents an aggregate function.
		/// </summary>
		/// <remarks>
		/// An aggregate function requires the IGroupResolver variable 
		/// to be present in able to resolve the function over some set.
		/// The result of an aggregate function is guaranteed the same 
		/// given the same set and identical parameters.
		/// </remarks>
		Aggregate = 2,

		/// <summary>
		/// A function that is non-aggregate but whose return value is not 
		/// guaranteed to be the same given the identical parameters over 
		/// subsequent calls.
		/// </summary>
		/// <remarks>
		/// This would include functions such as RANDOM and UNIQUEKEY. The 
		/// result is dependent on some other state (a random seed and a 
		/// sequence value).
		/// </remarks>
		StateBased = 3,

		UserDefined = 4,
		External = 5
	}
}