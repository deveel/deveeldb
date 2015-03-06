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

namespace Deveel.Data.Store {
	/// <summary>
	/// Defines a referenced object that can be accessed
	/// on a multi-phase level.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Implementations of this interface are large objects
	/// established in specialized data stores, which value
	/// is not retrievied immediately.
	/// </para>
	/// <para>
	/// A large object is uniquely referenced using the <see cref="Id"/>
	/// value that is obtaining when establishing the object in a store.
	/// </para>
	/// </remarks>
	public interface ILargeObject : IDisposable {
		/// <summary>
		/// Gets the unique identifier of the object within the system.
		/// </summary>
		ObjectId Id { get; }

		/// <summary>
		/// Gets the raw byte size of the object.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Large objects are pre-allocated in stores, that means this
		/// value represents the maximum size of the object, defined
		/// at creation.
		/// </para>
		/// <para>
		/// The returned value of this property is also variable by the kind
		/// of compression applied to the store (if <see cref="IsCompressed"/> is
		/// <c>true</c>).
		/// </para>
		/// </remarks>
		long RawSize { get; }

		/// <summary>
		/// Gets a value indicating whether the object is compressed.
		/// </summary>
		bool IsCompressed { get; }

		/// <summary>
		/// Gets a value indicating if the object is in its
		/// complete state, that means it cannot be written further,
		/// but it can only be read.
		/// </summary>
		/// <seealso cref="Complete"/>
		bool IsComplete { get; }

		/// <summary>
		/// Reads the content of the object, starting at a given offset,
		/// into the buffer given, for the number of bytes specified.
		/// </summary>
		/// <param name="offset">The zero-based offset within the object at
		/// which to start reading the contents.</param>
		/// <param name="buffer">The array in which to write the contents read.</param>
		/// <param name="length">The desired number of bytes to read from the
		/// object contents.</param>
		/// <returns>
		/// Returns the actual number of bytes read from the object.
		/// </returns>
		int Read(long offset, byte[] buffer, int length);

		/// <summary>
		/// Write the given binary content into the object, starting at
		/// the given offset for the number of bytes specified.
		/// </summary>
		/// <param name="offset">The zero-based starting offset at which to start
		/// to write the specified contents.</param>
		/// <param name="buffer">The content to write to the underlying object.</param>
		/// <param name="length">The number of bytes from the given buffer to write into
		/// the object.</param>
		void Write(long offset, byte[] buffer, int length);

		/// <summary>
		/// Marks the object as complete.
		/// </summary>
		/// <remarks>
		/// <para>
		/// After this method is invoked, the object is marked
		/// as complete and it cannot be written further.
		/// </para>
		/// <para>
		/// Any call to <see cref="Read"/> before this method is called
		/// will throw an exception.
		/// </para>
		/// </remarks>
		/// <seealso cref="IsComplete"/>
		/// <seealso cref="Read"/>
		/// <seealso cref="Write"/>
		void Complete();

		/// <summary>
		/// Establishes a reference of the object to the
		/// underlying store which contains it.
		/// </summary>
		/// <remarks>
		/// A single object can be referenced multiple times within a store,
		/// and this prevents it to be removed from the store if it is still
		/// in use within the system.
		/// </remarks>
		/// <seealso cref="Release"/>
		void Establish();

		/// <summary>
		/// Removes a reference of the object from the underlying
		/// store which contains it.
		/// </summary>
		/// <remarks>
		/// The act of removing a reference of an object from the containing
		/// store does not automatically destroys it: in fact, this happens
		/// only if all references established for the object have been released.
		/// </remarks>
		/// <returns>
		/// Returns <c>true</c> if the object was removed from the store at
		/// its release, or <c>false</c> if it was retained.
		/// </returns>
		/// <seealso cref="Establish"/>
		bool Release();
	}
}