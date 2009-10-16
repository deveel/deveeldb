//  
//  IStoreDataAccessor.cs
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
	/// An interface for low level store data access methods.
	/// </summary>
	/// <remarks>
	/// This is used to implement a variety of ways of accessing data 
	/// from some resource, such as a file in a filesystem.  For example, 
	/// we might use this to access a file using the I/O API. Alternatively 
	/// we may use it to implement a scattering store that includes data 
	/// across multiple files in the filesystem.
	/// </remarks>
	interface IStoreDataAccessor : IDisposable {
		/// <summary>
		/// Returns true if the resource exists.
		/// </summary>
		bool Exists { get; }

		/// <summary>
		/// Deletes the data area resource.
		/// </summary>
		/// <returns>
		/// Returns true if the delete was successful.
		/// </returns>
		bool Delete();

		/// <summary>
		/// Opens the underlying data area representation.
		/// </summary>
		/// <param name="read_only"></param>
		/// <remarks>
		/// If the resource doesn't exist then it is created and the 
		/// size is set to 0.
		/// </remarks>
		void Open(bool read_only);

		/// <summary>
		/// Closes the underlying data area representation.
		/// </summary>
		void Close();

		/// <summary>
		/// Reads a block of data from the underlying data area at the given 
		/// position into the byte array at the given offset.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="len"></param>
		/// <returns>
		/// Returns the actual count of bytes read.
		/// </returns>
		int Read(long position, byte[] buf, int off, int len);

		/// <summary>
		/// Writes a block of data to the underlying data area from the byte 
		/// array at the given offset.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="len"></param>
		void Write(long position, byte[] buf, int off, int len);

		/// <summary>
		/// Sets the size of the underlying data area to the given size.
		/// </summary>
		/// <param name="new_size"></param>
		/// <remarks>
		/// If the size of the data area is increased, the content between 
		/// the old size and the new size is implementation defined.
		/// </remarks>
		void SetSize(long new_size);


		/// <summary>
		/// Returns the current size of the underlying data area.
		/// </summary>
		long Size { get; }

		/// <summary>
		/// Synchronizes the data area by forcing any data out of the OS buffers 
		/// onto the disk.
		/// </summary>
		void Synch();
	}
}