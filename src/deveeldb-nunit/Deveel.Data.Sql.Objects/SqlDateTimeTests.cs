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

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	[Category("SQL Objects")]
	[Category("Date Time")]
	public class SqlDateTimeTests {
		[Test]
		[Category("Day To Second Interval")]
		public void Add_TimeSpan_NoDays() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var ts = new SqlDayToSecond(0, 2, 03, 0);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value.Add(ts));
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2001, result.Year);
			Assert.AreEqual(01, result.Month);
			Assert.AreEqual(03, result.Day);
			Assert.AreEqual(12, result.Hour);
			Assert.AreEqual(25, result.Minute);
		}

		[Test]
		[Category("Day To Second Interval")]
		public void Subtract_TimeSpan_NoDays() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var ts = new SqlDayToSecond(0, 2, 03, 0);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value.Subtract(ts));
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2001, result.Year);
			Assert.AreEqual(01, result.Month);
			Assert.AreEqual(03, result.Day);
			Assert.AreEqual(8, result.Hour);
			Assert.AreEqual(19, result.Minute);
		}

		[Test]
		[Category("Year To Month")]
		public void Add_MonthSpan() {
			var value = new SqlDateTime(2001, 11, 03, 10, 22, 03, 0);
			var ms = new SqlYearToMonth(1, 3);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value.Add(ms));
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2003, result.Year);
			Assert.AreEqual(02, result.Month);
			Assert.AreEqual(10, result.Hour);
			Assert.AreEqual(22, result.Minute);
			Assert.AreEqual(03, result.Second);
			Assert.AreEqual(0, result.Millisecond);
		}

		[Test]
		[Category("Year To Month")]
		public void Subtract_MonthSpan() {
			var value = new SqlDateTime(2001, 11, 03, 10, 22, 03, 0);
			var ms = new SqlYearToMonth(1, 3);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value.Subtract(ms));
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2000, result.Year);
			Assert.AreEqual(08, result.Month);
			Assert.AreEqual(10, result.Hour);
			Assert.AreEqual(22, result.Minute);
			Assert.AreEqual(03, result.Second);
			Assert.AreEqual(0, result.Millisecond);			
		}

		[Test]
		[Category("Operators")]
		[Category("Day To Second Interval")]
		public void Operator_Add_TimeSpan() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var ts = new SqlDayToSecond(0, 2, 03, 0);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value + ts);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2001, result.Year);
			Assert.AreEqual(01, result.Month);
			Assert.AreEqual(03, result.Day);
			Assert.AreEqual(12, result.Hour);
			Assert.AreEqual(25, result.Minute);			
		}

		[Test]
		[Category("Day To Second Interval")]
		[Category("Operators")]
		public void Operator_Subtract_TimeSpan() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var ts = new SqlDayToSecond(0, 2, 03, 0);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value - ts);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2001, result.Year);
			Assert.AreEqual(01, result.Month);
			Assert.AreEqual(03, result.Day);
			Assert.AreEqual(8, result.Hour);
			Assert.AreEqual(19, result.Minute);
		}

		[Test]
		[Category("Operators")]
		[Category("Year To Month")]
		public void Operator_Add_MonthSpan() {
			var value = new SqlDateTime(2001, 11, 03, 10, 22, 03, 0);
			var ms = new SqlYearToMonth(1, 3);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value + ms);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2003, result.Year);
			Assert.AreEqual(02, result.Month);
			Assert.AreEqual(10, result.Hour);
			Assert.AreEqual(22, result.Minute);
			Assert.AreEqual(03, result.Second);
			Assert.AreEqual(0, result.Millisecond);
		}

		[Test]
		[Category("Year To Month")]
		[Category("Operators")]
		public void Operator_Subtract_MonthSpan() {
			var value = new SqlDateTime(2001, 11, 03, 10, 22, 03, 0);
			var ms = new SqlYearToMonth(1, 3);

			var result = new SqlDateTime();
			Assert.DoesNotThrow(() => result = value - ms);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2000, result.Year);
			Assert.AreEqual(08, result.Month);
			Assert.AreEqual(10, result.Hour);
			Assert.AreEqual(22, result.Minute);
			Assert.AreEqual(03, result.Second);
			Assert.AreEqual(0, result.Millisecond);
		}


		[Test]
		[Category("Operators")]
		public void Equality_True() {
			var value1 = new SqlDateTime(2030, 03, 01, 11, 05, 54, 0);
			var value2 = new SqlDateTime(2030, 03, 01, 11, 05, 54, 0);

			Assert.IsTrue(value1 == value2);
		}

		[Test]
		[Category("Operators")]
		public void Inequality_True() {
			var value1 = new SqlDateTime(2030, 03, 01, 11, 05, 54, 0);
			var value2 = new SqlDateTime(2020, 05, 01, 11, 05, 54, 0);

			Assert.IsTrue(value1 != value2);
		}

		[Test]
		[Category("Operators")]
		public void Greater_True() {
			var value1 = new SqlDateTime(2030, 03, 01, 11, 05, 54, 0);
			var value2 = new SqlDateTime(2020, 05, 01, 11, 05, 54, 0);
			
			Assert.IsTrue(value1 > value2);
		}

		[Test]
		[Category("Operators")]
		public void Lesser_True() {
			var value1 = new SqlDateTime(2030, 03, 01, 11, 05, 54, 0);
			var value2 = new SqlDateTime(2020, 05, 01, 11, 05, 54, 0);

			Assert.IsTrue(value2 < value1);
		}

		[Test]
		[Category("Operators")]
		public void Lesser_False() {
			var value1 = new SqlDateTime(2030, 03, 01, 11, 05, 54, 0);
			var value2 = new SqlDateTime(2020, 05, 01, 11, 05, 54, 0);

			Assert.IsFalse(value1 < value2);
		}

		[TestCase(true)]
		[TestCase(false)]
		[Category("Conversion")]
		public void ToByteArray(bool timeZone) {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var bytes = value.ToByteArray(timeZone);

			var expectedLength = timeZone ? 13 : 11;

			Assert.IsNotNull(bytes);
			Assert.AreNotEqual(0, bytes.Length);
			Assert.AreEqual(expectedLength, bytes.Length);

			var rebuilt = new SqlDateTime(bytes);

			Assert.AreEqual(value, rebuilt);
		}

		[Test]
		[Category("Conversion")]
		public void ToDateTime() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var date = value.ToDateTime();

			Assert.AreEqual(2001, date.Year);
			Assert.AreEqual(01, date.Month);
			Assert.AreEqual(03, date.Day);
			Assert.AreEqual(10, date.Hour);
			Assert.AreEqual(22, date.Minute);
			Assert.AreEqual(03, date.Second);
			Assert.AreEqual(0, date.Millisecond);
		}


		[Test]
		[Category("Conversion")]
		public void ToDateTimeOffset_WithoutOffset() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0);
			var date = value.ToDateTimeOffset();

			Assert.AreEqual(2001, date.Year);
			Assert.AreEqual(01, date.Month);
			Assert.AreEqual(03, date.Day);
			Assert.AreEqual(10, date.Hour);
			Assert.AreEqual(22, date.Minute);
			Assert.AreEqual(03, date.Second);
			Assert.AreEqual(0, date.Millisecond);
			Assert.AreEqual(0, date.Offset.Hours);
			Assert.AreEqual(0, date.Offset.Minutes);
		}

		[Test]
		[Category("Conversion")]
		public void ToDateTimeOffset_WithOffset() {
			var value = new SqlDateTime(2001, 01, 03, 10, 22, 03, 0, new SqlDayToSecond(2, 30, 0));
			var date = value.ToDateTimeOffset();

			Assert.AreEqual(2001, date.Year);
			Assert.AreEqual(01, date.Month);
			Assert.AreEqual(03, date.Day);
			Assert.AreEqual(10, date.Hour);
			Assert.AreEqual(22, date.Minute);
			Assert.AreEqual(03, date.Second);
			Assert.AreEqual(0, date.Millisecond);
			Assert.AreEqual(2, date.Offset.Hours);
			Assert.AreEqual(30, date.Offset.Minutes);
		}

		[Test]
		[Category("Conversion")]
		public void FromDateTimeOffset_WithOffset_Implicit() {
			var date = new DateTimeOffset(2001, 01, 03, 10, 22, 03, 0, new TimeSpan(2, 30, 0));
			var value = (SqlDateTime) date;

			Assert.AreEqual(2001, value.Year);
			Assert.AreEqual(01, value.Month);
			Assert.AreEqual(03, value.Day);
			Assert.AreEqual(10, value.Hour);
			Assert.AreEqual(22, value.Minute);
			Assert.AreEqual(03, value.Second);
			Assert.AreEqual(0, value.Millisecond);
			Assert.AreEqual(2, value.Offset.Hours);
			Assert.AreEqual(30, value.Offset.Minutes);
		}

		[Test]
		[Category("Conversion")]
		public void FromNullDateTimeOffset_Implicit() {
			DateTimeOffset? date = null;
			var value = (SqlDateTime) date;

			Assert.IsNotNull(value);
			Assert.IsTrue(value.IsNull);
			Assert.AreEqual(SqlDateTime.Null, value);
		}

		[Test]
		public void ParseTimeStamp_NoTimeZone() {
			const string s = "2016-04-22T12:34:11.432";

			SqlDateTime dateTime;
			Assert.IsTrue(SqlDateTime.TryParseTimeStamp(s, out dateTime));

			Assert.AreEqual(2016, dateTime.Year);
			Assert.AreEqual(04, dateTime.Month);
			Assert.AreEqual(22, dateTime.Day);
			Assert.AreEqual(12, dateTime.Hour);
			Assert.AreEqual(34, dateTime.Minute);
			Assert.AreEqual(11, dateTime.Second);
			Assert.AreEqual(432, dateTime.Millisecond);
			Assert.AreEqual(0, dateTime.Offset.Hours);
			Assert.AreEqual(0, dateTime.Offset.Minutes);
		}

		[Test]
		public void ParseTimeStamp_WithTimeZone() {
			const string s = "2016-04-22T12:34:11.432";
			var timeZone = TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time");

			SqlDateTime dateTime;
			Assert.IsTrue(SqlDateTime.TryParseTimeStamp(s, timeZone, out dateTime));

			Assert.AreEqual(2016, dateTime.Year);
			Assert.AreEqual(04, dateTime.Month);
			Assert.AreEqual(22, dateTime.Day);
			Assert.AreEqual(12, dateTime.Hour);
			Assert.AreEqual(34, dateTime.Minute);
			Assert.AreEqual(11, dateTime.Second);
			Assert.AreEqual(432, dateTime.Millisecond);
			Assert.AreEqual(2, dateTime.Offset.Hours);
			Assert.AreEqual(0, dateTime.Offset.Minutes);
		}

		[Test]
		public void AtCetTimeZone() {
			var value = new SqlDateTime(2016, 12, 03, 22, 45, 0, 0);
			var dateTime = value.AtTimeZone("CET");

			Assert.AreEqual(2016, dateTime.Year);
			Assert.AreEqual(12, dateTime.Month);
			Assert.AreEqual(03, dateTime.Day);
			Assert.AreEqual(23, dateTime.Hour);
			Assert.AreEqual(45, dateTime.Minute);
			Assert.AreEqual(0, dateTime.Second);
			Assert.AreEqual(0, dateTime.Millisecond);
			Assert.AreEqual(1, dateTime.Offset.Hours);
			Assert.AreEqual(0, dateTime.Offset.Minutes);
		}
	}
}