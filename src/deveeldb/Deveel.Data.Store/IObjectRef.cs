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
	public interface IObjectRef : IDisposable {
		/// <summary>
		/// Gets the unique identifier of the object within the system.
		/// </summary>
		ObjectId Id { get; }

		/// <summary>
		/// Gets the type of object referenced.
		/// </summary>
		ObjectType Type { get; }

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
		/// This value describes the length of the object in bytes as stored,
		/// that is not always comparable to the length of a string: in fact,
		/// if the <see cref="Type">type</see> of this object is <see cref="ObjectType.UnicodeString"/>
		/// the number of characters stored will be double as the size of the
		/// object (since <c>UNICODE</c> strings are 2-byte notations).
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
	}
}