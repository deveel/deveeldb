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
	/// An interface for low level store data access methods.
	/// </summary>
	public interface IStoreData : IDisposable {
		/// <summary>
		/// Gets a value indicating whether the data block object exists.
		/// </summary>
		/// <remarks>
		/// The existence of this object is relative to the kind of
		/// store the data block belongs to.
		/// </remarks>
		bool Exists { get; }

		/// <summary>
		/// Gets the current length of the data block.
		/// </summary>
		long Length { get; }

		/// <summary>
		/// Gets a value indicating whether the data block is in read-only mode.
		/// </summary>
		/// <remarks>
		/// When a block is read-only any call to <see cref="Write"/>
		/// will throw an exception.
		/// </remarks>
		bool IsReadOnly { get; }

		/// <summary>
		/// Deletes the data block.
		/// </summary>
		/// <returns>
		/// Returns <c>true</c> if the block was successfully deleted,
		/// or <c>false</c> otherwise.
		/// </returns>
		bool Delete();

		/// <summary>
		/// Opens the data block and make it ready to be accessed.
		/// </summary>
		/// <param name="readOnly">Indicates if the block must be open
		/// in read-only mode.</param>
		void Open(bool readOnly);

		/// <summary>
		/// Closes the block and make it unavailable.
		/// </summary>
		/// <remarks>
		/// When <see cref="IDisposable.Dispose"/> is invoked this method
		/// is also called to prevent any operation before disposal.
		/// </remarks>
		void Close();

		/// <summary>
		/// Reads a given amount of data from the block, starting at the absolute
		/// position given and copying into the provided buffer.
		/// </summary>
		/// <param name="position">The absolute position within the data block from
		/// where to start reading.</param>
		/// <param name="buffer">The destination buffer where the data read will be filled in.</param>
		/// <param name="offset">The starting offset within the buffer where to start copying
		/// the data read.</param>
		/// <param name="length">The desired number of bytes to read from the data block.</param>
		/// <returns>
		/// Returns the actual number of bytes read from the data block or 0 if the end
		/// of the block was reached.
		/// </returns>
		int Read(long position, byte[] buffer, int offset, int length);

		/// <summary>
		/// Writes a given buffer into the block, starting at the absolute position given.
		/// </summary>
		/// <param name="position">The absolute position within the data block from
		/// where to start writing.</param>
		/// <param name="buffer">The data to write into the block.</param>
		/// <param name="offset">The starting offset within the buffer where to start
		/// writing data from.</param>
		/// <param name="length">The number of bytes to write into the data block.</param>
		void Write(long position, byte[] buffer, int offset, int length);

		/// <summary>
		/// Flushes the data written in the temporary store
		/// of the block to the underlying medium.
		/// </summary>
		void Flush();

		/// <summary>
		/// Sets the length of the data block.
		/// </summary>
		/// <param name="value">The new size to set for the data block.</param>
		/// <remarks>
		/// If the <paramref name="value"/> is less than <see cref="Length"/> this
		/// method will shrink the data block and trim the exceeding contents. If the
		/// <see cref="value"/> is more than <see cref="Length"/> then it is responsibility
		/// of the implementation of this contract to increase the contents.
		/// </remarks>
		void SetLength(long value);
	}
}