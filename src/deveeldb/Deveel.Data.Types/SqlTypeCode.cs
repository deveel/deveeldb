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
		Numeric = 2,

		///<summary>
		/// A 128-bit decimal number with floating point.
		///</summary>
		Decimal = 3,

		///<summary>
		/// Defines a character type with  fixed size given
		///</summary>
		Char = 1,

		///<summary>
		/// The character type with a variable size, within a maximum
		/// size given.
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
		/// A user-defined generic object type.
		///</summary>
		Object = 2000,

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

		///<summary>
		/// A boolean type that can store either <c>true</c>
		/// or <c>false</c> values (0 or 1).
		///</summary>
		Boolean = 16,

		/// <summary>
		/// A long string in the system.
		/// </summary>
		String = 20,

		///<summary>
		///</summary>
		QueryPlanNode = -19443,

		RowType = -1256,
		ColumnType = -1257,

		///<summary>
		/// An unknown SQL type (either not supported by the system.
		///</summary>
		Unknown = -9332,
	}
}
