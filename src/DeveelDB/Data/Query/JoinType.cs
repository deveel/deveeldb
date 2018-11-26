// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Query {
	/// <summary>
	/// Enumerates the kind of group join in a selection query.
	/// </summary>
	public enum JoinType {
		/// <summary>
		/// A join that requires both sources joined to have
		/// matching records.
		/// </summary>
		Inner = 1,

		/// <summary>
		/// Returns all the records in the left side of the join, even if
		/// the other side has no corresponding records.
		/// </summary>
		Left = 2,

		/// <summary>
		/// Returns all the records in the right side of the join, even if
		/// the other side has no corresponding records.
		/// </summary>
		Right = 3,

		/// <summary>
		/// 
		/// </summary>
		Full = 4,

		/// <summary>
		/// Defaults to the natural join between two sources.
		/// </summary>
		None = -1,
	}
}