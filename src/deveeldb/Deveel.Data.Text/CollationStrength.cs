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

namespace Deveel.Data.Text {
	public enum CollationStrength {
		/// <summary>
		/// Only primary differences between characters will be 
		/// considered signficant.
		/// </summary>
		/// <remarks>
		/// As an example, two completely different English letters 
		/// such as 'a' and 'b' are considered to have a primary difference.
		/// </remarks>
		Primary    = 0,

		/// <summary>
		/// Only secondary or primary differences between characters 
		/// will be considered significant.
		/// </summary>
		/// <remarks>
		/// An example of a secondary difference between characters
		/// are instances of the same letter with different accented forms.
		/// </remarks>
		Secondary  = 1,

		/// <summary>
		/// Tertiary, secondary, and primary differences will be considered 
		/// during sorting.
		/// </summary>
		/// <remarks>
		/// An example of a tertiary difference is capitalization of a given 
		/// letter. This is the default value for the strength setting.
		/// </remarks>
		Tertiary   = 2,

		Quaternary = 3,

		/// <summary>
		/// Any difference at all between character values are considered 
		/// significant.
		/// </summary>
		Identical  = 15,

		None = -1
	}
}