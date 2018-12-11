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
using System.IO;

namespace Deveel.Data.Storage {
	public interface IStore : IDisposable {
		StoreState State { get; }

		/// <summary>
		/// Allocates a block of memory in the store of the specified size 
		/// and returns an <see cref="IArea"/> object that can be used 
		/// to initialize the contents of the area.
		/// </summary>
		/// <param name="size">The amount of memory to allocate.</param>
		/// <remarks>
		/// Note that an area in the store is undefined until the <see cref="IArea.Flush"/>
		/// method is called in <see cref="IArea"/>.
		/// </remarks>
		/// <returns>
		/// Returns an <see cref="IArea"/> object that allows the area to be setup.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// If not enough space available to create the area or the store is Read-only.
		/// </exception>
		IArea CreateArea(long size);

		/// <summary>
		/// Deletes an area that was previously allocated by the <see cref="CreateArea"/>
		/// method by the area id.
		/// </summary>
		/// <param name="id">The identifier of the area to delete.</param>
		/// <remarks>
		/// Once an area is deleted the resources may be reclaimed. The behaviour of this 
		/// method is undefined if the id doesn't represent a valid area.
		/// </remarks>
		/// <exception cref="IOException">
		/// If the id is invalid or the area can not otherwise by deleted.
		/// </exception>
		void DeleteArea(long id);

		/// <summary>
		/// Returns an object that allows for the contents of an area (represented 
		/// by the <paramref name="id"/> parameter) to be Read.
		/// </summary>
		/// <param name="id">The identifier of the area to Read, or -1 for a 64 byte 
		/// fixed area in the store.</param>
		/// <param name="readOnly">Indicates if the returned area must be read-only.</param>
		/// <remarks>
		/// The behaviour of this method is undefined if the id doesn't represent a valid area.
		/// <para>
		/// When <paramref name="id"/> is -1 then a fixed area (64 bytes in size) in the store is 
		/// returned. The fixed area can be used to store important static information.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an <see cref="IArea"/> object that allows access to the part of the store.
		/// </returns>
		/// <exception cref="IOException">
		/// If the id is invalid or the area can not otherwise be accessed.
		/// </exception>
		IArea GetArea(long id, bool readOnly);

		/// <summary>
		/// This method is called before the start of a sequence of Write commands 
		/// between consistant states of some data structure represented by the store.
		/// </summary>
		/// <remarks>
		/// This Lock mechanism is intended to inform the store when it is not safe to
		/// checkpoint the data in a log, ensuring that no 
		/// partial updates are committed to a transaction log and the data can be 
		/// restored in a consistant manner.
		/// <para>
		/// If the store does not implement a check point log or is otherwise not
		/// interested in consistant states of the data, then it is not necessary for
		/// this method to do anything.
		/// </para>
		/// <para>
		/// This method prevents a check point from happening during some sequence of
		/// operations. This method should not Lock unless a check point is in progress.
		/// This method does not prevent concurrent writes to the store.
		/// </para>
		/// </remarks>
		/// <seealso cref="Unlock"/>
		void Lock();

		/// <summary>
		/// This method is called after the end of a sequence of Write commands 
		/// between consistant states of some data structure represented by the store.
		/// </summary>
		/// <remarks>
		/// See the <see cref="Lock"/> method for a further description of the 
		/// operation of this locking mechanism.
		/// </remarks>
		/// <seealso cref="Lock"/>
		void Unlock();
	}
}