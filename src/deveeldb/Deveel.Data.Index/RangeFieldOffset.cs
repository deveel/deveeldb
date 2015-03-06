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

namespace Deveel.Data.Index {
	/// <summary>
	/// The absolute offset of a field in a range of a selection.
	/// </summary>
	/// <seealso cref="IndexRange"/>
	public enum RangeFieldOffset {
		/// <summary>
		/// The offset of the first value of the range. 
		/// </summary>
		FirstValue = 1,

		/// <summary>
		/// The offset of the last value of the range.
		/// </summary>
		LastValue = 2,

		/// <summary>
		/// The offset before the first value of the range.
		/// </summary>
		BeforeFirstValue = 3,

		/// <summary>
		/// The offset after the last value of the range.
		/// </summary>
		AfterLastValue = 4
	}
}