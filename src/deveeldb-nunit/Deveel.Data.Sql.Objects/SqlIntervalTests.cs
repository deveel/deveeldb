using System;

using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	public class SqlIntervalTests {
		[TestCase("2015-02-16", "", "")]
		public void ResultOfIntervalAdd(string dateString, string intervalString, SqlTypeCode typeCode, string resultString) {
			var date = SqlDateTime.Parse(dateString);
			var interval = SqlDayToSecond.Parse(intervalString);

			var result = date.Add(interval);
		}

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
	}
}
