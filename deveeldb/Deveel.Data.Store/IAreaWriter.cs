// 
//  IAreaWriter.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;

namespace Deveel.Data.Store {
	/// <summary>
	/// The interface used for setting up an area initially in a store.
	/// </summary>
	/// <remarks>
	/// This method is intended to optimize the area creation process. Typically 
	/// an area is created at a specified size and filled with data.
	/// <para>
	/// Note that an area may only be written sequentially using this object.  This
	/// is by design and allows for the area initialization process to be optimized.
	/// </para>
	/// </remarks>
	/// <example>
	/// This area should be used as follows:
	/// <code>
	///     IAreaWriter writer = store.CreateArea(16);
	///     writer.WriteInt4(3);
	///     writer.WriteInt8(100030);
	///     writer.WriteByte(1);
	///     writer.WriteInt2(0);
	///     writer.WriteByte(2);
	///     writer.Finish();
	/// </code>
	/// When the <see cref="Finish"/> method is called, the IAreaWriter object is invalidated 
	/// and the area can then be accessed in the store by the <see cref="IStore.GetArea"/> method.
	/// </example>
	public interface IAreaWriter {
		/// <summary>
		/// Returns the unique identifier that represents this area in the store.
		/// </summary>
		/// <value>
		/// The id is -1 if the area is the store's static area.  Otherwise the
		/// id is a positive number that will not exceed 60 bits of the long.
		/// </value>
		long Id { get; }


		/// <summary>
		/// Returns a <see cref="Stream"/> that can be used to Write to this area.
		/// </summary>
		/// <remarks>
		/// This stream is backed by this area writer, so if 10 bytes area written to the
		/// output stream then the writer position is also incremented by 10 bytes.
		/// </remarks>
		/// <returns></returns>
		Stream GetOutputStream();


		/// <summary>
		/// Returns the size of this area.
		/// </summary>
		int Capacity { get; }

		/// <summary>
		/// Finishes the area writer object.
		/// </summary>
		/// <remarks>
		/// This must be called when the area is completely initialized. After 
		/// this method is called the object is invalidated and the area can be 
		/// accessed in the store.
		/// </remarks>
		void Finish();

		// ---------- Various WriteByte methods ----------

		/// <summary>
		/// Writes a single byte into the underlying area.
		/// </summary>
		/// <param name="value">The byte value to Write.</param>
		void WriteByte(byte value);

		void Write(byte[] buf, int off, int len);

		void Write(byte[] buf);

		void WriteInt2(short value);

		void WriteInt4(int value);

		void WriteInt8(long value);

		void WriteChar(char value);

	}
}