//  
//  IJournalledResource.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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