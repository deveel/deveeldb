// 
//  Copyright 2011  Deveel
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
using System.Collections.Generic;

namespace Deveel.Data.Index {
	/// <summary>
	/// Enumerates the elements of an index.
	/// </summary>
	/// <remarks>
	/// Additionally to the functionalities inherited from <see cref="IEnumerator{T}"/>,
	/// it provides the backward direction (<see cref="MoveBack"/>) and the removal
	/// from the underlying <see cref="IIndex"/> of the element at the current position 
	/// of the enumeration (<see cref="Remove"/>).
	/// </remarks>
	public interface IIndexEnumerator : IEnumerator<int> {
		///<summary>
		/// Reverses the direction of the enumerator to the previous
		/// element within the list.
		///</summary>
		///<returns>
		/// Returns <b>true</b> if the enumerator has more elements when traversing 
		/// the index in the reverse direction, otherwise it returns <b>false</b>.
		/// </returns>
		bool MoveBack();

		///<summary>
		/// Removes from the underlying index the current element this enumerator
		/// is positioned at.
		///</summary>
		/// <remarks>
		/// This method can be called only once per call to <see cref="IEnumerator{T}.Current"/>. 
		/// The behavior of an iterator is unspecified if the underlying index is modified while the 
		/// iteration is in progress in any way other than by calling this method.
		/// <para>
		/// Some implementations of <see cref="IIndexEnumerator"/> may choose to not implement 
		/// this method, in which case an appropriate exception is generated.
		/// </para>
		/// </remarks>
		void Remove();
	}
}