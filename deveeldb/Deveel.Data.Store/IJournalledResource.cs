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

namespace Deveel.Data.Store {
	/// <summary>
	/// An interface that allows for the reading and writing of pages 
	/// to/from a journalled.
	/// </summary>
	interface IJournalledResource {
		/// <summary>
		/// Returns the page size.
		/// </summary>
		int PageSize { get; }

		/// <summary>
		/// Returns a unique id for this resource.
		/// </summary>
		long Id { get; }

		/// <summary>
		/// Reads a page of some previously specified size into the byte array.
		/// </summary>
		/// <param name="page_number"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		void Read(long page_number, byte[] buf, int off);

		/// <summary>
		/// Writes a page of some previously specified size to the top log.
		/// </summary>
		/// <param name="page_number"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="len"></param>
		/// <remarks>
		/// This will add a single entry to the log and any 'Read' operations after 
		/// will contain the written data.
		/// </remarks>
		void Write(long page_number, byte[] buf, int off, int len);

		/// <summary>
		/// Sets the new size of the resource.
		/// </summary>
		/// <param name="size"></param>
		/// <remarks>
		/// This will add a single entry to the log.
		/// </remarks>
		void SetSize(long size);

		/// <summary>
		/// Returns the current size of this resource.
		/// </summary>
		long Size { get; }

		/// <summary>
		/// Opens the resource.
		/// </summary>
		/// <param name="read_only"></param>
		void Open(bool read_only);

		/// <summary>
		/// Closes the resource.
		/// </summary>
		/// <remarks>
		/// This will actually simply log that the resource has been closed.
		/// </remarks>
		void Close();

		/// <summary>
		/// Deletes the resource.
		/// </summary>
		/// <remarks>
		/// This will actually simply log that the resource has been deleted.
		/// </remarks>
		void Delete();

		/// <summary>
		/// Returns true if the resource currently exists.
		/// </summary>
		bool Exists { get; }
	}
}