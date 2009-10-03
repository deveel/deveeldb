// 
//  DbTypes.cs
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

namespace Deveel.Data {
	/// <summary>
	/// The possible types used in the database.
	/// </summary>
	public enum DbTypes {

		DB_UNKNOWN = -1,

		DB_STRING = 1,
		DB_NUMERIC = 2,
		DB_TIME = 3,
		[Obsolete("Use BLOB instead.")]
		DB_BINARY = 4,
		DB_BOOLEAN = 5,
		DB_BLOB = 6,
		DB_OBJECT = 7,

		// This is an extended numeric type that handles neg and positive infinity
		// and NaN.
		DB_NUMERIC_EXTENDED = 8

	}
}