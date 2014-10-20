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
	}
}