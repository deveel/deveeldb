// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

namespace Deveel.Data.Types {
	/// <summary>
	/// Enumerates the codes of all SQL types handled by the system.
	/// </summary>
	public enum SqlTypeCode {
		///<summary>
		/// An unknown SQL type (either not supported by the system.
		///</summary>
		Unknown = 0,

		///<summary>
		/// The <c>NULL</c> object type.
		///</summary>
		Null = 1,

		///<summary>
		/// A boolean type that can store either <c>true</c>
		/// or <c>false</c> values (0 or 1).
		///</summary>
		Boolean = 9,

		/// <summary>
		/// A single byte that can have only 1 and 0
		/// as values.
		/// </summary>
		Bit = 10,

		/// <summary>
		/// An integer value that can span from 0 to 255.
		/// </summary>
		TinyInt = 11,

		/// <summary>
		/// An integer number of 2 bytes.
		/// </summary>
		SmallInt = 12,

		///<summary>
		/// A 4-bytes long integer type.
		///</summary>
		Integer = 13,

		/// <summary>
		/// A 8-bytes long integer type.
		/// </summary>
		BigInt = 14,

		///<summary>
		/// A 4-bytes numeric value with a floating point.
		///</summary>
		Float = 15,

		///<summary>
		///</summary>
		Real = 16,

		///<summary>
		/// A 8-bytes numeric value with a floating point.
		///</summary>
		Double = 17,

		/// <summary>
		/// A generic numeric type. This is the main type used
		/// by the system to represent numbers.
		/// </summary>
		Numeric = 18,

		///<summary>
		/// A 128-bit decimal number with floating point.
		///</summary>
		Decimal = 19,

		///<summary>
		/// Defines a character type with  fixed size given
		///</summary>
		Char = 20,

		///<summary>
		/// The character type with a variable size, within a maximum
		/// size given.
		///</summary>
		VarChar = 21,

		///<summary>
		///</summary>
		LongVarChar = 22,

		///<summary>
		/// A type that can store either large amount of ASCII or 
		/// UNICODE character data.
		///</summary>
		Clob = 23,

		/// <summary>
		/// A long string in the system.
		/// </summary>
		String = 24,

		///<summary>
		///</summary>
		Date = 30,

		///<summary>
		///</summary>
		Time = 31,

		///<summary>
		///</summary>
		TimeStamp = 32,

		YearToMonth = 33,

		DayToSecond = 34,

		///<summary>
		///</summary>
		Binary = 50,

		///<summary>
		///</summary>
		VarBinary = 51,

		///<summary>
		///</summary>
		LongVarBinary = 52,

		///<summary>
		/// A type that can store large amount of binary data.
		///</summary>
		Blob = 53,

		///<summary>
		/// A user-defined generic object type.
		///</summary>
		Object = 70,

		Geometry = 73,
		Xml = 74,
		UserType = 75,

		///<summary>
		///</summary>
		Array = 80,

		///<summary>
		///</summary>
		QueryPlan = 100,

		RowType = 111,
		ColumnType = 112,
	}
}
