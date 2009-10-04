//  
//  IMutableArea.cs
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
	/// An interface for an area that can be modified.
	/// </summary>
	/// <remarks>
	/// Any changes made to an area may or may not be immediately reflected in 
	/// already open areas with the same id.  The specification does guarantee 
	/// that after the <see cref="CheckOut"/> method is invoked that any new 
	/// <see cref="IArea"/> or <see cref="IMutableArea"/> objects created by the 
	/// backing store will contain the changes.
	/// </remarks>
	public interface IMutableArea : IArea {
		/// <summary>
		/// Checks out all changes made to this area.
		/// </summary>
		/// <remarks>
		/// This should be called after a series of updates have been made to the 
		/// area and the final change is to be 'finalized'.  When this method returns, 
		/// any new <see cref="IArea"/> or <see cref="IMutableArea"/> objects created by 
		/// the backing store will contain the changes made to this object.  Any changes 
		/// made to the <see cref="IArea"/> may or may not be made to any already existing 
		/// areas.
		/// <para>
		/// In a logging implementation, this may flush out the changes made to the area 
		/// in a log.
		/// </para>
		/// </remarks>
		void CheckOut();

		// ---------- Various write methods ----------

		void WriteByte(byte value);

		void Write(byte[] buf, int off, int len);

		void Write(byte[] buf);

		void WriteInt2(short value);

		void WriteInt4(int value);

		void WriteInt8(long value);

		void WriteChar(char value);
	}
}