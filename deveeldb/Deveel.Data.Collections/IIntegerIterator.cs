//  
//  IIntegerIterator.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Collections {
	/// <summary>
	/// Interface for iterating through an integer collection or block lists.
	/// </summary>
	/// <remarks>
	/// This interface has a similar layout to <see cref="IEnumerator"/>
	/// because represents an enumerator of a collection of integers. It 
	/// differs from the above for the strong-typed layout and for the 
	/// backward direction (<see cref="MovePrevious"/>).
	/// It also allow the deletion of the current value (<see cref="Remove"/>).
	/// </remarks>
	public interface IIntegerIterator {
		///<summary>
		/// Advances the iteration to the next element within the list.
		///</summary>
		///<returns>
		/// Returns <b>true</b> if the iterator has more elements when traversing 
		/// the list in the forward direction, otherwise it returns <b>false</b>.
		/// </returns>
		bool MoveNext();

		///<summary>
		/// Returns the next element in the list.
		///</summary>
		/// <remarks>
		/// Alternating calls to <see cref="Next"/> and <see cref="Previous"/> will 
		/// return the same element repeatedly.
		/// </remarks>
		int Next { get; }

		///<summary>
		/// Reverses the direction of the iteration to the previous
		/// element within the list.
		///</summary>
		///<returns>
		/// Returns <b>true</b> if the iterator has more elements when traversing 
		/// the list in the reverse direction, otherwise it returns <b>false</b>.
		/// </returns>
		bool MovePrevious();

		///<summary>
		/// Returns the previous element in the list.
		///</summary>
		/// <remarks>
		/// Alternating calls to <see cref="Next"/> and <see cref="Previous"/> will 
		/// return the same element repeatedly.
		/// </remarks>
		int Previous { get; }

		///<summary>
		/// Removes from the list the last element returned by the iterator.
		///</summary>
		/// <remarks>
		/// This method can be called only once per call to <see cref="Next"/>. The behavior 
		/// of an iterator is unspecified if the underlying collection is modified while the 
		/// iteration is in progress in any way other than by calling this method.
		/// <para>
		/// Some implementations of <see cref="IIntegerIterator"/> may choose to not implement 
		/// this method, in which case an appropriate exception is generated.
		/// </para>
		/// </remarks>
		void Remove();

	}
}