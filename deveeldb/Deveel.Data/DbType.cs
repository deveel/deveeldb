//  
//  DbType.cs
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
	/// The types of data handled by the database system.
	/// </summary>
	/// <remarks>
	/// This enumeration specifies all the data types
	/// that the system can manage: SQL specifications
	/// define a wider range of types (eg. <c>VARCHAR</c>,
	/// <c>INTEGER</c>, <c>TIME</c>, etc.), while the
	/// system encapsulates each type into a domain generic
	/// type.
	/// </remarks>
	public enum DbType {
		/// <summary>
		/// An unknown database type.
		/// </summary>
		Unknown = -1,

		/// <summary>
		/// A generic medium-string type.
		/// </summary>
		/// <remarks>
		/// This type can handle strings of smaller size: 
		/// for greater sizes it will be necessary to use
		/// <see cref="Blob"/> type.
		/// </remarks>
		String = 1,

		///<summary>
		/// A generic numeric type that handles all the
		/// numeric values stored into the system.
		///</summary>
		Numeric = 2,

		/// <summary>
		/// The time type that handles dates and times within
		/// the database system.
		/// </summary>
		Time = 3,

		///<summary>
		///</summary>
		[Obsolete("Use BLOB instead.")]
		Binary = 4,

		///<summary>
		/// A type that handles boolean types (either <c>true</c>
		/// or <c>false</c>).
		///</summary>
		Boolean = 5,

		///<summary>
		/// The type used to handle binary data in a database.
		///</summary>
		/// <remarks>
		/// This is also used to store CLOB data.
		/// </remarks>
		Blob = 6,

		///<summary>
		///</summary>
		Object = 7,

		///<summary>
		/// An extended numeric type that handles neg and positive 
		/// infinity and NaN.
		///</summary>
		NumericExtended = 8

	}
}