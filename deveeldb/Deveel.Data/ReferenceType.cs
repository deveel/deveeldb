//  
//  ReferenceType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

namespace Deveel.Data {
	/// <summary>
	/// The possible type of a <see cref="IRef"/> object.
	/// </summary>
	/// <remarks>
	/// This enumeration includes the member <see cref="Compressed"/>
	/// that can be used to mark other kind of references as
	/// compressed.
	/// </remarks>
	[Flags]
	public enum ReferenceType : byte {
		/// <summary>
		/// This kind of reference handles binary data.
		/// </summary>
		Binary = 2,

		/// <summary>
		/// This kind of reference manages text data in ASCII format.
		/// </summary>
		AsciiText = 3,

		/// <summary>
		/// This kind of reference manages text data in UTF-16 format.
		/// </summary>
		UnicodeText = 4,

		/// <summary>
		/// A flag that marks a reference as compressed.
		/// </summary>
		Compressed = 0x010
	}
}