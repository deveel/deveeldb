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
	/// An interface for access the contents of an area of a store.
	/// </summary>
	public interface IArea {
		/// <summary>
		/// Returns the unique identifier that represents this area.
		/// </summary>
		/// <value>
		/// The id is -1 if the area is the store's static area. Otherwise the id is 
		/// a positive number that will not exceed 60 bits of the long.
		/// </value>
		long Id { get; }

		/// <summary>
		/// Gets a value indicating whether this area can be written or not.
		/// </summary>
		/// <remarks>
		/// If this area is read-only, any attempt to call <see cref="Write"/>
		/// method will throw an exception.
		/// </remarks>
		bool IsReadOnly { get; }

		/// <summary>
		/// Returns or sets the current position of the pointer within the area.
		/// </summary>
		/// <remarks>
		/// The position starts at beginning of the area.
		/// </remarks>
		int Position { get; set; }

		/// <summary>
		/// Returns the capacity of the area.
		/// </summary>
		int Capacity { get; }

		int Length { get; }

		/// <summary>
		/// Copies the given amount of bytes from the current position of the
		/// this area to another one.
		/// </summary>
		/// <param name="destArea">The <see cref="IArea"/> where to write.</param>
		/// <param name="size">The number of bytes to Write.</param>
		void CopyTo(IArea destArea, int size);

		/// <summary>
		/// Reads an array of bytes from the underlying <see cref="IArea"/>
		/// and advances the position by <paramref name="length"/>.
		/// </summary>
		/// <param name="buffer">The destination buffer into which to Read the
		/// number of bytes given from the area.</param>
		/// <param name="offset">The offset within the buffer from where to
		/// start writing the byte Read into.</param>
		/// <param name="length">The number of bytes to Read from the area. This
		/// is also the incremental size of the position of the area.</param>
		/// <returns>
		/// Returns the number of bytes actually Read from the <see cref="IArea"/>.
		/// </returns>
		int Read(byte[] buffer, int offset, int length);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		void Write(byte[] buffer, int offset, int length);

		void Flush();
	}
}