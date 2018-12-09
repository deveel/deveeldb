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
using System.Collections.Generic;

namespace Deveel.Collections {
	/// <summary>
	/// Enumerates the elements of an collection.
	/// </summary>
	/// <remarks>
	/// Additionally to the functionalities inherited from <see cref="IEnumerator{T}"/>,
	/// it the removal from the underlying <see cref="ISortedCollection{TKey,TValue}"/> of the element 
	/// at the current position of the enumeration (using <see cref="Remove"/>).
	/// </remarks>
	public interface ISortedCollectionEnumerator<T> : IEnumerator<T> {
		///<summary>
		/// Removes from the underlying collection the current element this enumerator
		/// is positioned at.
		///</summary>
		/// <remarks>
		/// This method can be called only once per call to <see cref="IEnumerator{T}.Current"/>. 
		/// The behavior of an iterator is unspecified if the underlying collection is modified while the 
		/// iteration is in progress in any way other than by calling this method.
		/// <para>
		/// Some implementations of <see cref="ISortedCollectionEnumerator{T}"/> may choose to not implement 
		/// this method, in which case an appropriate exception is generated.
		/// </para>
		/// </remarks>
		void Remove();
	}
}