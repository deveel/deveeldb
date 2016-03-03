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

namespace Deveel.Data.Index {
	/// <summary>
	/// A comparer that is used within <see cref="IIndex{T}"/> to 
	/// compares two values which are indices to data that is being compared.
	/// </summary>
	/// <remarks>
	/// For example, an instance of <see cref="IIndex{T}"/> may contains 
	/// indices to cells in the column of a table. This object is used to make sorted lists, 
	/// to lookup the index values in the list for sorting and searching.
	/// </remarks>
	public interface IIndexComparer<T> {
		/// <summary>
		/// Compares a value contained in the underlying index
		/// at the given position and the 
		/// </summary>
		/// <param name="index">The offset within the underlying index
		/// at which to get the first value to compare.</param>
		/// <param name="value">The value to compare with the one retrieved
		/// from the underlying index at the given position.</param>
		/// <returns>
		/// Returns > 0 if the value pointed to by index1 is greater than 'val',
		/// or &lt; 0 if the value pointed to by index 1 is less than 'val'.  If the
		/// indexed value is equal to 'val', it returns 0.
		/// </returns>
		int CompareValue(T index, Field value);

		/// <summary>
		/// Comnpares two values referenced by the indices given.
		/// </summary>
		/// <param name="index1">The offset within the underlying index
		/// at which to get the first value to compare.</param>
		/// <param name="index2">The offset within the underlying index
		/// at which to get the second value to compare.</param>
		/// <returns>
		/// Returns an integer greather than 0 if the value pointed to by <paramref name="index1"/>
		/// is greater than the value pointed to by <paramref name="index2"/>, or an integer
		/// smaller than 0 if the value pointed to by <paramref name="index1"/> 1 is less
		/// than the value pointed to by <paramref name="index2"/>. If the indexed values are equal,
		/// it returns 0.
		/// </returns>
		int Compare(T index1, T index2);
	}
}