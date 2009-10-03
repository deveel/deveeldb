// 
//  IIndexComparer.cs
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
using System.Collections;

namespace Deveel.Data.Collections {
	/// <summary>
	/// A comparer that is used within <see cref="BlockIntegerList"/> that 
	/// compares two integer values which are indices to data that is being 
	/// compared.
	/// </summary>
	/// <remarks>
	/// For example, we may have an <see cref="BlockIntegerList"/> that contains 
	/// indices to cells in the column of a table.  To make a sorted list, we use 
	/// this comparator to lookup the index values in the list for sorting and searching.
	/// </remarks>
	public interface IIndexComparer : IComparer {
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
		int Compare(int index1, object val);

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