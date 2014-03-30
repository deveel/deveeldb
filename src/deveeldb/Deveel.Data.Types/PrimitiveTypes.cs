// 
//  Copyright 2010 -2014 Deveel
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

using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	public static class PrimitiveTypes {
		/// <summary>
		/// A default boolean (SQL Bit) type.
		/// </summary>
		public static readonly TBooleanType Boolean = new TBooleanType(SqlType.Bit);

		/// <summary>
		/// A default date (SQL TIMESTAMP) type.
		/// </summary>
		public static readonly TDateType Date = new TDateType(SqlType.TimeStamp);

		/// <summary>
		/// A default NULL type.
		/// </summary>
		public static readonly TNullType Null = new TNullType();

		/// <summary>
		/// A default numeric (SQL NUMERIC) type of unlimited size and scale.
		/// </summary>
		public static readonly TNumericType Numeric = new TNumericType(SqlType.Numeric, -1, -1);

		/// <summary>
		/// A type that represents a query plan (sub-select).
		/// </summary>
		public static readonly TQueryPlanType QueryPlan = new TQueryPlanType();

		/// <summary>
		/// A default binary (SQL BLOB) type of unlimited maximum size.
		/// </summary>
		public static readonly TBinaryType BinaryType = new TBinaryType(SqlType.Blob, -1);

		/// <summary>
		/// A default string (SQL VARCHAR) type of unlimited maximum size and
		/// null locale.
		/// </summary>
		public static readonly TStringType VarString = new TStringType(SqlType.VarChar, -1, null);

		public static readonly TIntervalType YearToMonth = new TIntervalType(SqlType.YearToMonth);

		public static readonly TIntervalType DayToSecond = new TIntervalType(SqlType.DayToSecond);

		/// <summary>
		/// A default time-span (SQL INTERVAL) type.
		/// </summary>
		public static readonly TIntervalType IntervalType = new TIntervalType(SqlType.Interval);

		/// <summary>
		/// A type that represents an array.
		/// </summary>
		public static readonly TArrayType ArrayType = new TArrayType();
	}
}