//  
//  SqlType.cs
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

namespace Deveel.Data {
	/// <summary>
	/// All the types that can be specified in a SQL statement.
	/// </summary>
	/// <remarks>
	/// The members of this enumeration represent the data types
	/// specified by the SQL standards, although the system uses
	/// an higher level for data-types (eg. <c>NUMERIC</c>, <c>STRING</c>,
	/// <c>BOOLEAN</c>, etc.), as defined in <see cref="DbType"/>.
	/// </remarks>
	/// <see cref="DbType"/>
	public enum SqlType {
		/// <summary>
		/// A single byte that can have only 1 and 0
		/// as values.
		/// </summary>
		Bit = -7,

		/// <summary>
		/// An integer value that can span from 0 to 255.
		/// </summary>
		TinyInt = -6,

		/// <summary>
		/// An integer number of 2 bytes.
		/// </summary>
		SmallInt = 5,

		///<summary>
		/// A 4-bytes long integer type.
		///</summary>
		Integer = 4,

		/// <summary>
		/// A 8-bytes long integer type.
		/// </summary>
		BigInt = -5,

		///<summary>
		/// A 4-bytes numeric value with a floating point.
		///</summary>
		Float = 6,

		///<summary>
		///</summary>
		Real = 7,

		///<summary>
		/// A 8-bytes numeric value with a floating point.
		///</summary>
		Double = 8,

		/// <summary>
		/// A generic numeric type. This is the main type used
		/// by the system to represent numbers.
		/// </summary>
		/// <seealso cref="DbType.Numeric"/>
		Numeric = 2,

		///<summary>
		/// A 128-bit decimal number with floating point.
		///</summary>
		Decimal = 3,

		///<summary>
		///</summary>
		Char = 1,

		///<summary>
		///</summary>
		VarChar = 12,

		///<summary>
		///</summary>
		LongVarChar = -1,

		///<summary>
		///</summary>
		Date = 91,

		///<summary>
		///</summary>
		Time = 92,

		///<summary>
		///</summary>
		TimeStamp = 93,

		///<summary>
		///</summary>
		Interval = 100,

		YearToMonth = 101,

		DayToSecond = 102,

		///<summary>
		///</summary>
		Binary = -2,

		///<summary>
		///</summary>
		VarBinary = -3,

		///<summary>
		///</summary>
		LongVarBinary = -4,

		///<summary>
		/// The <c>NULL</c> object type.
		///</summary>
		Null = 0,

		///<summary>
		///</summary>
		Other = 1111,

		///<summary>
		/// A user-defined generic object type.
		///</summary>
		Object = 2000,

		///<summary>
		///</summary>
		Distinct = 2001,

		///<summary>
		///</summary>
		Struct = 2002,

		///<summary>
		///</summary>
		Array = 2003,

		///<summary>
		/// A type that can store large amount of binary data.
		///</summary>
		Blob = 2004,

		///<summary>
		/// A type that can store either large amount of ASCII or 
		/// UNICODE character data.
		///</summary>
		Clob = 2005,

		/// <summary>
		/// A generic type that references portion of data from
		/// a BLOB store within the system.
		/// </summary>
		Ref = 2006,

		///<summary>
		/// A boolean type that can store either <c>true</c>
		/// or <c>false</c> values (0 or 1).
		///</summary>
		/// <seealso cref="DbType.Boolean"/>
		Boolean = 16,

		///<summary>
		///</summary>
		QueryPlanNode = -19443,

		///<summary>
		/// An unknown SQL type (either not supported by the system.
		///</summary>
		Unknown = -9332,

		/// <summary>
		/// The type used to define an auto-incremental numeric
		/// column type.
		/// </summary>
		Identity = 56
	}
}