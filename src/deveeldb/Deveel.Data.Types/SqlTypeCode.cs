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
		/// The <c>NULL</c> object type.
		///</summary>
		Null = 0,

		/// <summary>
		/// A single byte that can have only 1 and 0
		/// as values.
		/// </summary>
		Bit,

		/// <summary>
		/// An integer value that can span from 0 to 255.
		/// </summary>
		TinyInt,

		/// <summary>
		/// An integer number of 2 bytes.
		/// </summary>
		SmallInt,

		///<summary>
		/// A 4-bytes long integer type.
		///</summary>
		Integer,

		/// <summary>
		/// A 8-bytes long integer type.
		/// </summary>
		BigInt,

		///<summary>
		/// A 4-bytes numeric value with a floating point.
		///</summary>
		Float,

		///<summary>
		///</summary>
		Real,

		///<summary>
		/// A 8-bytes numeric value with a floating point.
		///</summary>
		Double,

		/// <summary>
		/// A generic numeric type. This is the main type used
		/// by the system to represent numbers.
		/// </summary>
		Numeric,

		///<summary>
		/// A 128-bit decimal number with floating point.
		///</summary>
		Decimal,

		///<summary>
		/// Defines a character type with  fixed size given
		///</summary>
		Char,

		///<summary>
		/// The character type with a variable size, within a maximum
		/// size given.
		///</summary>
		VarChar,

		///<summary>
		///</summary>
		LongVarChar,

		///<summary>
		///</summary>
		Date,

		///<summary>
		///</summary>
		Time,

		///<summary>
		///</summary>
		TimeStamp,

		YearToMonth,

		DayToSecond,

		///<summary>
		///</summary>
		Binary,

		///<summary>
		///</summary>
		VarBinary,

		///<summary>
		///</summary>
		LongVarBinary,

		///<summary>
		/// A user-defined generic object type.
		///</summary>
		Object,

		///<summary>
		///</summary>
		Array,

		///<summary>
		/// A type that can store large amount of binary data.
		///</summary>
		Blob,

		///<summary>
		/// A type that can store either large amount of ASCII or 
		/// UNICODE character data.
		///</summary>
		Clob,

		///<summary>
		/// A boolean type that can store either <c>true</c>
		/// or <c>false</c> values (0 or 1).
		///</summary>
		Boolean,

		/// <summary>
		/// A long string in the system.
		/// </summary>
		String,

		///<summary>
		///</summary>
		QueryPlanNode,

		RowType,
		ColumnType,

		///<summary>
		/// An unknown SQL type (either not supported by the system.
		///</summary>
		Unknown,
		Geometry,
		UserType,
	}
}
