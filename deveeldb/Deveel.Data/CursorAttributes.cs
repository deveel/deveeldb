//  
//  CursorAttributes.cs
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
	/// The flags defining the properties of a <see cref="Cursor"/>.
	/// </summary>
	[Flags]
	public enum CursorAttributes {
		/// <summary>
		/// Marks a cursor as read-only: this is the exact opposite
		/// of the option <see cref="Update"/>.
		/// </summary>
		ReadOnly = 0x01,

		/// <summary>
		/// A cursor marked with this flag can update a given set of
		/// columns of a table or environment variables.
		/// </summary>
		Update = 0x02,

		/// <summary>
		/// The cursor will ignore every modification made to the tables
		/// referenced by the query command forming it after its declaration.
		/// </summary>
		Insensitive = 0x04,

		/// <summary>
		/// Allows fetching directions other than forward.
		/// </summary>
		Scrollable = 0x05
	}
}