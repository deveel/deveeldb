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

using System;

namespace Deveel.Data.Text {
	/// <summary>
	/// Options used in the <see cref="CollationKey.GetBound"/> for getting a 
	/// <see cref="CollationKey"/> based on the bound mode requested.
	/// </summary>
	public enum CollationBoundMode {
		Lower     = 0,

		/// <summary>
		/// Upper bound that will match strings of exact size.
		/// </summary>
		Upper     = 1,

		/// <summary>
		/// Upper bound that will match all the strings that have the same 
		/// initial substring as the given string.
		/// </summary>
		UpperLong = 2,

		Count     = 3,
	}
}