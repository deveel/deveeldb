// 
//  Copyright 2010-2014 Deveel
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
using System.Globalization;
using System.Text;

using Deveel.Data.Sql.Objects;
using Deveel.Math;

using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Deveel.Data.Sql.Types {
	[TestFixture]
	[Category("Data Types")]
	public class StringTypeTests {
		[Test]
		public void BasicVarChar_Create() {
			var type = PrimitiveTypes.String(SqlTypeCode.VarChar);
			Assert.AreEqual(SqlTypeCode.VarChar, type.TypeCode);
			Assert.AreEqual(Int16.MaxValue, type.MaxSize);
			Assert.IsTrue(type.IsPrimitive);
			Assert.IsTrue(type.IsIndexable);
			Assert.IsNull(type.Locale);
		}

		[Test]
		public void BasicVarChar_Compare() {
			var type1 = PrimitiveTypes.String(SqlTypeCode.VarChar);
			var type2 = PrimitiveTypes.String(SqlTypeCode.VarChar);

			Assert.AreEqual(type1.TypeCode, type2.TypeCode);
			Assert.IsTrue(type1.IsComparable(type2));
			Assert.IsTrue(type1.CanCastTo(type2));
		}

		[TestCase("VARCHAR",  SqlTypeCode.VarChar, Int16.MaxValue, null, null)]
		[TestCase("VARCHAR(2002)", SqlTypeCode.VarChar, 2002, null, null)]
		[TestCase("CHAR", SqlTypeCode.Char, Int16.MaxValue, null, null)]
		[TestCase("STRING", SqlTypeCode.String, Int16.MaxValue, null, null)]
		[TestCase("LONG CHARACTER VARYING", SqlTypeCode.LongVarChar, Int16.MaxValue, null, null)]
		[TestCase("VARCHAR(456) LOCALE 'en-US'", SqlTypeCode.VarChar, 456, "en-US", null)]
		[TestCase("VARCHAR LOCALE 'nb-NO' ENCODING 'utf-8'", SqlTypeCode.VarChar, Int16.MaxValue, "nb-NO", "utf-8")]
		[Category("Strings"), Category("SQL Parse")]
		public void ParseString(string input, SqlTypeCode expectedTypeCode, int expectedSize, string expectedLocale, string expectedEncoding) {
			var sqlType = SqlType.Parse(input);

			Assert.IsNotNull(sqlType);
			Assert.IsInstanceOf<StringType>(sqlType);
			Assert.AreEqual(expectedTypeCode, sqlType.TypeCode);

			var stringType = (StringType)sqlType;
			Assert.AreEqual(expectedSize, stringType.MaxSize);

			var locale = String.IsNullOrEmpty(expectedLocale) ? null : new CultureInfo(expectedLocale);
			Assert.AreEqual(locale, stringType.Locale);

			var encoding = String.IsNullOrEmpty(expectedEncoding) ? Encoding.Unicode : Encoding.GetEncoding(expectedEncoding);
			Assert.AreEqual(encoding, stringType.Encoding);
		}

		[Test]
		public void SizedVarChar_Create() {
			var type = PrimitiveTypes.String(SqlTypeCode.VarChar, 255);
			Assert.AreEqual(SqlTypeCode.VarChar, type.TypeCode);
			Assert.AreEqual(255, type.MaxSize);
		}

		[Test]
		public void SizedVarChar_Compare() {
			var type1 = PrimitiveTypes.String(SqlTypeCode.VarChar, 255);
			var type2 = PrimitiveTypes.String(SqlTypeCode.VarChar, 200);

			Assert.AreEqual(type1.TypeCode, type2.TypeCode);
			Assert.IsFalse(type1.Equals(type2));
			Assert.IsTrue(type1.IsComparable(type2));
		}

		[TestCase("1", true)]
		[TestCase("0", false)]
		[TestCase("false", false)]
		[TestCase("true", true)]
		[TestCase("TRUE", true)]
		[TestCase("FALSE", false)]
		[TestCase("tRuE", true)]
		public void CastToBoolean(string s, bool expected) {
			var type = PrimitiveTypes.String();
			var value = new SqlString(s);

			var casted = type.CastTo(value, PrimitiveTypes.Boolean());

			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlBoolean>(casted);

			Assert.AreEqual(expected, (bool)(SqlBoolean)casted);
		}

		[TestCase("2015-03-03", 2015, 03, 03, 0, 0, 0, 0, null, null)]
		[TestCase("2014-02-28 22:18:03", 2014, 2, 28, 22, 18, 3, 0, null, null)]
		[TestCase("2016-05-19T09:22:11.556 +02:00", 2016, 05, 19, 09, 22, 11, 556, 02, 00)]
		[TestCase("02:11:04.232", 01, 01, 01, 02, 11, 04, 232, null, null)]
		[TestCase("1890-02-15", 1890, 02, 15, 0, 0, 0, 0, null, null)]
		public void CastToDateTime(string s, int year, int month, int day, int hour, int minute, int second, int millis,
			int? offsetHour, int? offsetMinute) {

			var type = PrimitiveTypes.String();
			var value = new SqlString(s);

			var casted = type.CastTo(value, PrimitiveTypes.DateTime());

			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlDateTime>(casted);

			var date = (SqlDateTime) casted;
			Assert.AreEqual(year, date.Year);
			Assert.AreEqual(month, date.Month);
			Assert.AreEqual(day, date.Day);
			Assert.AreEqual(hour, date.Hour);
			Assert.AreEqual(minute, date.Minute);
			Assert.AreEqual(second, date.Second);
			Assert.AreEqual(millis, date.Millisecond);

			var offset = (offsetHour != null && offsetMinute != null)
				? new SqlDayToSecond(0, offsetHour.Value, offsetMinute.Value, 0)
				: SqlDayToSecond.Zero;

			Assert.AreEqual(offset, date.Offset);
		}

		[TestCase("1492-10-12", 1492, 10, 12)]
		public void CastToDate(string s, int year, int month, int day) {

			var type = PrimitiveTypes.String();
			var value = new SqlString(s);

			var casted = type.CastTo(value, PrimitiveTypes.Date());

			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlDateTime>(casted);

			var date = (SqlDateTime)casted;
			Assert.AreEqual(year, date.Year);
			Assert.AreEqual(month, date.Month);
			Assert.AreEqual(day, date.Day);
			Assert.AreEqual(0, date.Hour);
			Assert.AreEqual(0, date.Minute);
			Assert.AreEqual(0, date.Second);
			Assert.AreEqual(0, date.Millisecond);
			Assert.AreEqual(SqlDayToSecond.Zero, date.Offset);
		}

		[TestCase("02:11:04.232", 02, 11, 04, 232, null, null)]
		[TestCase("13:25:26.444 +03:30", 13, 25, 26, 444, 03, 30)]
		public void CastToTime(string s, int hour, int minute, int second, int millis, int? offsetHour, int? offsetMinute) {
			var type = PrimitiveTypes.String();
			var value = new SqlString(s);

			var casted = type.CastTo(value, PrimitiveTypes.Time());

			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlDateTime>(casted);

			var date = (SqlDateTime)casted;
			Assert.AreEqual(01, date.Year);
			Assert.AreEqual(01, date.Month);
			Assert.AreEqual(01, date.Day);
			Assert.AreEqual(hour, date.Hour);
			Assert.AreEqual(minute, date.Minute);
			Assert.AreEqual(second, date.Second);
			Assert.AreEqual(millis, date.Millisecond);

			var offset = (offsetHour != null && offsetMinute != null)
				? new SqlDayToSecond(0, offsetHour.Value, offsetMinute.Value, 0)
				: SqlDayToSecond.Zero;

			Assert.AreEqual(offset, date.Offset);
		}

		[TestCase("34499", 34499, true, true)]
		[TestCase("789919992", 789919992, false, true)]
		[TestCase("56.0993", 56.0993, false, false)]
		[TestCase("-67378", -67378, true, true)]
		public void CastToSimpleNumeric(string s, double expected, bool canBeInt, bool canBeLong) {
			var type = PrimitiveTypes.String();
			var value = new SqlString(s);

			var casted = type.CastTo(value, PrimitiveTypes.Numeric());

			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlNumber>(casted);

			var number = (SqlNumber) casted;
			Assert.AreEqual(expected, number.ToDouble());
			Assert.AreEqual(canBeInt, number.CanBeInt32);
			Assert.AreEqual(canBeLong, number.CanBeInt64);
		}
	}
}