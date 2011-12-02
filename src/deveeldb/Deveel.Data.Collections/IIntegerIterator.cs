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