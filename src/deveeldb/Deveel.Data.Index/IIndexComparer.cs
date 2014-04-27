// 
//  Copyright 2010-2011  Deveel
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

namespace Deveel.Data.Index {
	/// <summary>
	/// A comparer that is used within <see cref="IIndex{T}"/> that 
	/// compares two values which are indices to data that is being compared.
	/// </summary>
	/// <remarks>
	/// For example, we may have an <see cref="IIndex{T}"/> that contains 
	/// indices to cells in the column of a table. To make a sorted list, we use 
	/// this comparer to lookup the index values in the list for sorting and searching.
	/// </remarks>
	public interface IIndexComparer {
		/// <summary>
		/// 
		/// </summary>
		/// <param name="index1"></param>
		/// <param name="val"></param>
		/// <returns>
		/// Returns > 0 if the value pointed to by index1 is greater than 'val',
		/// or &lt; 0 if the value pointed to by index 1 is less than 'val'.  If the
		/// indexed value is equal to 'val', it returns 0.
		/// </returns>
		int CompareValue(int index1, TObject val);

		/// <summary>
		/// Comnpares two values referenced by the indices given.
		/// </summary>
		/// <param name="index1"></param>
		/// <param name="index2"></param>
		/// <returns>
		/// Returns >0 if the value pointed to by index1 is greater than the value
		/// pointed to by index2, or &lt; 0 if the value pointed to by index 1 is less
		/// than the value pointed to by index 2.  If the indexed value's are equal,
		/// it returns 0.
		/// </returns>
		int Compare(int index1, int index2);

	}
}