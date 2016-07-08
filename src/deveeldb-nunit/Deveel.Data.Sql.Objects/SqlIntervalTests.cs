using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	public class SqlIntervalTests {
		[TestCase("33.00:12:29.999", 33, 0, 12, 29, 999)]
		[TestCase("19:11:18.100", 0, 19, 11, 18, 100)]
		public void ParseDayToSecond(string s, int days, int hours, int minutes, int seconds, int millis) {
			SqlDayToSecond dayToSecond;
			Assert.IsTrue(SqlDayToSecond.TryParse(s, out dayToSecond));
			Assert.IsFalse(dayToSecond.IsNull);

			Assert.AreEqual(days, dayToSecond.Days);
			Assert.AreEqual(hours, dayToSecond.Hours);
			Assert.AreEqual(minutes, dayToSecond.Minutes);
			Assert.AreEqual(seconds, dayToSecond.Seconds);
			Assert.AreEqual(millis, dayToSecond.Milliseconds);
		}

		[TestCase(1, 14, 33, 22, 0, "1.14:33:22")]
		[TestCase(0, 23, 11, 08, 223, "23:11:08.2230000")]
		public void DayToSecondToString(int days, int hours, int minutes, int seconds, int millis, string expected) {
			var dayToSecond = new SqlDayToSecond(days, hours, minutes, seconds, millis);
			var s = dayToSecond.ToString();
			Assert.IsNotNullOrEmpty(s);
			Assert.AreEqual(expected, s);
		}

		[Test]
		public void AddDayToSecond() {
			var dts1 = new SqlDayToSecond(22, 11, 23, 0, 678);
			var dts2 = new SqlDayToSecond(0, 3, 22, 15, 877);

			var result = dts1.Add(dts2);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			Assert.AreEqual(22, result.Days);
			Assert.AreEqual(14, result.Hours);
			Assert.AreEqual(45, result.Minutes);
			Assert.AreEqual(16, result.Seconds);
			Assert.AreEqual(555, result.Milliseconds);
		}

		[Test]
		public void AddNullDayToSecond() {
			var dts1 = new SqlDayToSecond(22, 11, 23, 0, 678);
			var dts2 = SqlDayToSecond.Null;

			var result = dts1.Add(dts2);
			
			Assert.IsNotNull(result);
			Assert.IsTrue(result.IsNull);
		}

		[Test]
		public void AddDayToSecondToDate() {
			var date = new SqlDateTime(2010, 11, 03, 05, 22, 43, 0);
			var dts = new SqlDayToSecond(19, 08, 23, 1);

			var result = date.Add(dts);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(2010, result.Year);
			Assert.AreEqual(11, result.Month);
			Assert.AreEqual(22, result.Day);
			Assert.AreEqual(13, result.Hour);
			Assert.AreEqual(45, result.Minute);
			Assert.AreEqual(44, result.Second);
			Assert.AreEqual(0, result.Millisecond);
		}

		[Test]
		public void SubtractDayToSeconds() {
			var dts1 = new SqlDayToSecond(22, 11, 23, 0, 678);
			var dts2 = new SqlDayToSecond(0, 3, 22, 15, 877);

			var result = dts1.Subtract(dts2);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			Assert.AreEqual(22, result.Days);
			Assert.AreEqual(8, result.Hours);
			Assert.AreEqual(0, result.Minutes);
			Assert.AreEqual(44, result.Seconds);
			Assert.AreEqual(801, result.Milliseconds);
		}

		[Test]
		public void SubtractNullDayToSecond() {
			var dts1 = new SqlDayToSecond(22, 11, 23, 0, 678);
			var dts2 = SqlDayToSecond.Null;

			var result = dts1.Subtract(dts2);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.IsNull);
		}

		[Test]
		public void SubtractDayToSecondFromDate() {
			var date = new SqlDateTime(2010, 11, 03, 05, 22, 43, 0);
			var dts = new SqlDayToSecond(19, 08, 23, 1);

			var result = date.Subtract(dts);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			Assert.AreEqual(2010, result.Year);
			Assert.AreEqual(10, result.Month);
			Assert.AreEqual(14, result.Day);
			Assert.AreEqual(20, result.Hour);
			Assert.AreEqual(59, result.Minute);
			Assert.AreEqual(42, result.Second);
			Assert.AreEqual(0, result.Millisecond);
		}

		[Test]
		public void CompareTwoDayToSecond() {
			var dts1 = new SqlDayToSecond(22, 11, 23, 0, 678);
			var dts2 = new SqlDayToSecond(0, 3, 22, 15, 877);

			var result = dts1.CompareTo(dts2);

			Assert.AreEqual(1, result);
		}

		[TestCase(22, 1)]
		[Category("Year To Month")]
		[Category("Conversion")]
		public void YearToMonthFromInteger(int months, int expectedYears) {
			var value = new SqlYearToMonth(months);

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);

			Assert.AreEqual(expectedYears, (int) value.TotalYears);
		}

		[Test]
		[Category("Year To Month")]
		public void CompareYearToMonths() {
			var value1 = new SqlYearToMonth(22);
			var value2 = new SqlYearToMonth(1, 2);

			var result = value1.CompareTo(value2);
			Assert.AreEqual(1, result);
		}

		[Test]
		[Category("Year To Month")]
		public void CompareYearToMonthToNull() {
			var value1 = new SqlYearToMonth(22);
			var value2 = SqlYearToMonth.Null;

			var result = value1.CompareTo(value2);

			Assert.AreEqual(1, result);
		}
	}
}
