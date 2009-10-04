//  
//  IArea.cs
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
	/// An interface for access the contents of an area of a store.
	/// </summary>
	/// <remarks>
	/// The area object maintains a pointer that can be manipulated and Read from.
	/// </remarks>
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

		/// <summary>
		/// Copies the given amount of bytes from the current position of the
		/// current <see cref="IArea"/> to the given <see cref="IAreaWriter"/>.
		/// </summary>
		/// <param name="dest">The <see cref="IAreaWriter"/> where to Write.</param>
		/// <param name="size">The number of bytes to Write.</param>
		void CopyTo(IAreaWriter dest, int size);

		// ---------- The Read* methods ----------

		/// <summary>
		/// Reads a single byte from the underlying <see cref="IArea"/> and
		/// advances the position by one.
		/// </summary>
		/// <returns>
		/// Returns the byte Read from the <see cref="IArea"/>
		/// </returns>
		byte ReadByte();

		/// <summary>
		/// Reads an array of bytes from the underlying <see cref="IArea"/>
		/// and advances the position by <paramref name="len"/>.
		/// </summary>
		/// <param name="buf">The destination buffer into which to Read the
		/// number of bytes given from the area.</param>
		/// <param name="off">The offset within the buffer from where to
		/// start writing the byte Read into.</param>
		/// <param name="len">The number of bytes to Read from the area. This
		/// is also the incremental size of the position of the area.</param>
		/// <returns>
		/// Returns the number of bytes actually Read from the <see cref="IArea"/>.
		/// </returns>
		int Read(byte[] buf, int off, int len);

		/// <summary>
		/// Reads a short integer number from the underlying <see cref="IArea"/>
		/// and advances the position by two.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="short"/> which was Read from the underlying 
		/// <see cref="IArea"/> at the current position.
		/// </returns>
		short ReadInt2();

		/// <summary>
		/// Reads an integer number from the underlying <see cref="IArea"/> and
		/// advances the position by four.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="int"/> which was Read from the underlying
		/// <see cref="IArea"/> at the current position.
		/// </returns>
		int ReadInt4();

		long ReadInt8();

		char ReadChar();
	}
}