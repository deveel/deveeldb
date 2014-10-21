// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.Store {
	/// <summary>
	/// Defines the contract for stores that handle lrge objects
	/// within a database system.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Object stores manage a variety of <see cref="ObjectType"/> and must
	/// allow the creation and establishments of different type of objects
	/// (strings or binaries).
	/// </para>
	/// <para>
	/// Every object store has a unique identifier that is used
	/// to resolve the final address of a large object.
	/// </para>
	/// </remarks>
	public interface IObjectStore : IDisposable {
		/// <summary>
		/// Gets the unique identifier of the store within a database system.
		/// </summary>
		int Id { get; }

		/// <summary>
		/// Creates a new large object returning a reference to it.
		/// </summary>
		/// <param name="type">The type of object to create in the store.</param>
		/// <param name="maxSize">The maximum byte size that the object will use. This is
		/// a value to provide ahead of time and that will not change.</param>
		/// <param name="compressed">Indicates whether the created object contents
		/// will be compressed. Compression reduces the amount of size occupied
		/// by the object, but it will also affect performances when writing and reading.</param>
		/// <returns>
		/// Returns an instance of <see cref="IObjectRef"/> that is used to access the
		/// object stored.
		/// </returns>
		/// <exception cref="DataStorageException">
		/// If an error occurred when creating the object.
		/// </exception>
		IObjectRef CreateNewObject(ObjectType type, long maxSize, bool compressed);

		/// <summary>
		/// Gets an object that was previously created for the given 
		/// unique identifier.
		/// </summary>
		/// <param name="id">The unique identifier of the object to return.</param>
		/// <returns>
		/// Returns an instance of <see cref="IObjectRef"/> that references an
		/// object that was previously created by <see cref="CreateNewObject"/>.
		/// </returns>
		/// <exception cref="InvalidObjectIdException">
		/// If the given <paramref name="id"/> is outside the range of the store.
		/// </exception>
		/// <seealso cref="CreateNewObject"/>
		IObjectRef GetObject(ObjectId id);

		/// <summary>
		/// Marks the object identified as static within the the store.
		/// </summary>
		/// <param name="id">The unique identifier of the object for which 
		/// to establish the reference into the store.</param>
		/// <remarks>
		/// <para>
		/// This is used to count references to a blob, and possibly clean up a blob 
		/// if there are no references remaining to it.
		/// </para>
		/// </remarks>
		/// <exception cref="InvalidObjectIdException">
		/// If the given <paramref name="id"/> is outside the range of the store.
		/// </exception>
		/// <seealso cref="CreateNewObject"/>
		void EstablishReference(ObjectId id);

		/// <summary>
		/// Releases a static reference to the object identified from the store.
		/// </summary>
		/// <param name="id">The unique identifier of the object for which 
		/// to release the reference from the store.</param>
		void ReleaseReference(ObjectId id);
	}
}