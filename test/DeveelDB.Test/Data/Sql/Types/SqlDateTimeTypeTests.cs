// 
//  Copyright 2010-2018 Deveel
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
using System.IO;

using Deveel.Data.Serialization;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class SqlDateTimeTypeTests {
		[Theory]
		[InlineData(SqlTypeCode.Time)]
		[InlineData(SqlTypeCode.TimeStamp)]
		[InlineData(SqlTypeCode.DateTime)]
		[InlineData(SqlTypeCode.Date)]
		public static void GetValidDateTimeType(SqlTypeCode typeCode) {
			var type = new SqlDateTimeType(typeCode);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.True(type.IsIndexable);
			Assert.True(type.IsPrimitive);
			Assert.False(type.IsLargeObject);
			Assert.False(type.IsReference);
		}

		[Theory]
		[InlineData(SqlTypeCode.Time, "TIME")]
		[InlineData(SqlTypeCode.TimeStamp, "TIMESTAMP")]
		[InlineData(SqlTypeCode.DateTime, "DATETIME")]
		[InlineData(SqlTypeCode.Date, "DATE")]
		public static void GetString(SqlTypeCode typeCode, string expected) {
			var type = new SqlDateTimeType(typeCode);

			var s = type.ToString();
			Assert.Equal(expected, s);
		}

		// TODO:
     //   [Theory]
     //   [InlineData("TIME", SqlTypeCode.Time)]
     //   [InlineData("TIMESTAMP", SqlTypeCode.TimeStamp)]
     //   [InlineData("DATE", SqlTypeCode.Date)]
     //   [InlineData("DATETIME", SqlTypeCode.DateTime)]
	    //public static void ParseString(string s, SqlTypeCode typeCode) {
     //       var type = SqlType.Parse(s);

     //       Assert.NotNull(type);
     //       Assert.Equal(typeCode, type.TypeCode);
     //       Assert.IsType<SqlDateTimeType>(type);
     //   }

		[Theory]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.Time, "02:00:30.221")]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.TimeStamp, "2019-01-04T02:00:30.221")]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.Date, "2019-01-04")]
		public static void CastToDateTime(string s, SqlTypeCode destTypeCode, string expected) {
			var date = SqlDateTime.Parse(s);
			var type = new SqlDateTimeType(SqlTypeCode.DateTime);

			var destType = new SqlDateTimeType(destTypeCode);
			Assert.True(type.CanCastTo(date, destType));

			var result = type.Cast(date, destType);

			Assert.NotNull(result);
			Assert.IsType<SqlDateTime>(result);

			var expectedResult = SqlDateTime.Parse(expected);
			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.DateTime, SqlTypeCode.VarChar, -1, "2019-01-04T02:00:30.221 +00:00")]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.DateTime, SqlTypeCode.VarChar, 150, "2019-01-04T02:00:30.221 +00:00")]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.DateTime, SqlTypeCode.Char, 12, "2019-01-04T0")]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.DateTime, SqlTypeCode.Char, 32, "2019-01-04T02:00:30.221 +00:00  ")]
		[InlineData("02:00:30.221", SqlTypeCode.Time, SqlTypeCode.Char, 32, "02:00:30.221 +00:00             ")]
		[InlineData("02:00:30.221", SqlTypeCode.Time, SqlTypeCode.Char, 7, "02:00:3")]
		public static void CastToString(string s, SqlTypeCode typeCode, SqlTypeCode destTypeCode, int maxSize, string expected) {
			var date = SqlDateTime.Parse(s);
			var type = new SqlDateTimeType(typeCode);

			var destType = new SqlCharacterType(destTypeCode, maxSize, null);
			Assert.True(type.CanCastTo(date, destType));

			var result = type.Cast(date, destType);

			Assert.NotNull(result);
			Assert.IsType<SqlString>(result);

			Assert.Equal(expected, (SqlString) result);
		}

		[Theory]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.Numeric, 30, 10)]
		[InlineData("2019-01-04T02:00:30.221", SqlTypeCode.BigInt, 19, 0)]
		public static void CastToNumber(string s, SqlTypeCode typeCode, int precision, int scale) {
			var date = SqlDateTime.Parse(s);
			var type = new SqlDateTimeType(SqlTypeCode.DateTime);
			var destType = new SqlNumericType(typeCode, precision, scale);

			Assert.True(type.CanCastTo(date, destType));

			var result = type.Cast(date, destType);

			Assert.NotNull(result);
			Assert.IsType<SqlNumber>(result);

			var value = (SqlNumber) result;

			var back = new SqlDateTime((long)value);

			Assert.Equal(date, back);
		}

		[Theory]
		[InlineData("2016-11-29", "10.20:00:03.445", "2016-12-09T20:00:03.445")]
		public static void Add(string date, string offset, string expected) {
			BinaryWithInterval(type => type.Add, date, offset, expected);
		}

		[Theory]
		[InlineData("0001-02-10T00:00:01", "2.23:12:02", "0001-02-07T00:47:59")]
		public static void Subtract(string date, string offset, string expected) {
			BinaryWithInterval(type => type.Subtract, date, offset, expected);
		}

		[Theory]
		[InlineData("2011-02-10", "2001-10-11", false)]
		[InlineData("2016-01-01T04:20:56", "2016-01-01T04:20:56", true)]
		[InlineData("02:05:54.667", "02:05:54.668", false)]
		public static void Equal(string date1, string date2, bool expetced) {
			Binary(type => type.Equal, date1, date2, expetced);
		}

		[Theory]
		[InlineData("2011-02-10", "2001-10-11", true)]
		[InlineData("2016-01-01T04:20:56", "2016-01-01T04:20:56", false)]
		[InlineData("02:05:54.667", "02:05:54.668", true)]
		public static void NotEqual(string date1, string date2, bool expetced) {
			Binary(type => type.NotEqual, date1, date2, expetced);
		}

		[Theory]
		[InlineData("2010-03-10", "2001-10-11", true)]
		[InlineData("2016-01-01T04:20:56", "2016-01-01T04:20:56", false)]
		[InlineData("02:05:54.667", "02:05:54.668", false)]
		public static void Greater(string date1, string date2, bool expetced) {
			Binary(type => type.Greater, date1, date2, expetced);
		}

		[Theory]
		[InlineData("2010-03-10", "2001-10-11", true)]
		[InlineData("2016-01-01T04:20:56", "2016-01-01T04:20:56", true)]
		[InlineData("02:05:54.667", "02:05:54.668", false)]
		public static void GreaterOrEqual(string date1, string date2, bool expetced) {
			Binary(type => type.GreaterOrEqual, date1, date2, expetced);
		}

		[Theory]
		[InlineData("2010-03-10", "2001-10-11", false)]
		[InlineData("2016-01-01T04:20:56", "2016-01-01T04:20:56", false)]
		[InlineData("02:05:54.667", "02:05:54.668", true)]
		public static void Less(string date1, string date2, bool expetced) {
			Binary(type => type.Less, date1, date2, expetced);
		}

		[Theory]
		[InlineData("2010-03-10", "2001-10-11", false)]
		[InlineData("2016-01-01T04:20:56", "2016-01-01T04:20:56", true)]
		[InlineData("02:05:54.667", "02:05:54.668", true)]
		public static void LessOrEqual(string date1, string date2, bool expetced) {
			Binary(type => type.LessOrEqual, date1, date2, expetced);
		}


		private static void Binary(Func<SqlDateTimeType, Func<ISqlValue, ISqlValue, SqlBoolean>> selector, string date1, string date2, bool expected) {
			var type = new SqlDateTimeType(SqlTypeCode.DateTime);
			var sqlDate1 = SqlDateTime.Parse(date1);
			var sqlDate2 = SqlDateTime.Parse(date2);

			var op = selector(type);
			var result = op(sqlDate1, sqlDate2);

			var expectedResult = (SqlBoolean) expected;

			Assert.Equal(expectedResult, result);
		}

		private static void BinaryWithInterval(Func<SqlDateTimeType, Func<ISqlValue, ISqlValue, ISqlValue>> selector, string date, string offset, string expected) {
			var type = new SqlDateTimeType(SqlTypeCode.DateTime);
			var sqlDate = SqlDateTime.Parse(date);
			var dts = SqlDayToSecond.Parse(offset);

			var op = selector(type);
			var result = op(sqlDate, dts);

			var expectedResult = SqlDateTime.Parse(expected);

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData("2005-04-04", 5, "2005-09-04")]
		public static void AddMonths(string date, int months, string expected) {
			BinaryWithInterval(type => type.Add, date, months, expected);
		}

		[Theory]
		[InlineData("2013-12-01T09:11:25.893", 20, "2012-04-01T09:11:25.893")]
		public static void SubtractMonths(string date, int months, string expected) {
			BinaryWithInterval(type => type.Subtract, date, months, expected);
		}

		private static void BinaryWithInterval(Func<SqlDateTimeType, Func<ISqlValue, ISqlValue, ISqlValue>> selector, string date, int months, string expected)
		{
			var type = new SqlDateTimeType(SqlTypeCode.DateTime);
			var sqlDate = SqlDateTime.Parse(date);
			var ytm = new SqlYearToMonth(months);

			var op = selector(type);
			var result = op(sqlDate, ytm);

			var expectedResult = SqlDateTime.Parse(expected);

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(SqlTypeCode.Time)]
		[InlineData(SqlTypeCode.TimeStamp)]
		[InlineData(SqlTypeCode.DateTime)]
		[InlineData(SqlTypeCode.Date)]
		public static void Serialize(SqlTypeCode typeCode) {
			var type = new SqlDateTimeType(typeCode);
			var result = BinarySerializeUtil.Serialize(type);

			Assert.Equal(type, result);
		}
	}
}