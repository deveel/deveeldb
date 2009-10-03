// 
//  SearchResults.cs
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

namespace Deveel.Data.Util {
	///<summary>
	/// This object stores the result of a given search.
	///</summary>
	/// <remarks>
	/// It provides information object where in the set the found elements are, and 
	/// the number of elements in the set that match the search criteria.
	/// </remarks>
	public sealed class SearchResults {
		/// <summary>
		/// The index in the array of the found elements.
		/// </summary>
		internal int found_index;

		/// <summary>
		/// The number of elements in the array that match the given search criteria.
		/// </summary>
		internal int found_count;

		///<summary>
		///</summary>
		public SearchResults() {
		}

		///<summary>
		/// Returns he number of elements in the array that match the given 
		/// search criteria.
		///</summary>
		public int Count {
			get { return found_count; }
		}

		///<summary>
		/// Returns the index in the array of the found elements.
		///</summary>
		public int Index {
			get { return found_index; }
		}
	}
}