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


namespace Deveel.Data.Sql.Objects {
	/// <summary>
	/// Lists all the possible special states of a number.
	/// </summary>
	public enum NumericState : byte {
		/// <summary>
		/// The number has no special state, that means it's a normal
		/// number in the domain of real.
		/// </summary>
		None = 0,

		/// <summary>
		/// The number represents a negative infinity (for example,
		/// the result of a division of a negative number by zero).
		/// </summary>
		NegativeInfinity = 1,

		/// <summary>
		/// The number represents a positive infinity (for example,
		/// the result of a division of a positive number by zero).
		/// </summary>
		PositiveInfinity = 2,

		/// <summary>
		/// When the object is not a real number (for example, the result
		/// of a division of zero by zero).
		/// </summary>
		NotANumber = 3
	}
}