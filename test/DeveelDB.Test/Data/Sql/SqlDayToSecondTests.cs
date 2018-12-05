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

using Deveel.Data.Serialization;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlDayToSecondTests {
		[Theory]
		[InlineData(01, 22, 43, 33, 544)]
		public static void FromFullForm(int days, int hours, int minutes, int seconds, int millis) {
			var dts = new SqlDayToSecond(days, hours, minutes, seconds, millis);

			Assert.Equal(days, dts.Days);
			Assert.Equal(hours, dts.Hours);
			Assert.Equal(minutes, dts.Minutes);
			Assert.Equal(seconds, dts.Seconds);
			Assert.Equal(millis, dts.Milliseconds);
			Assert.True(dts.TotalMilliseconds > 0);
		}


		[Theory]
		[InlineData(0, 32, 21, 14, 778)]
		public static void GetBinary(int days, int hours, int minutes, int seconds, int millis) {
			var dts = new SqlDayToSecond(days, hours, minutes, seconds, millis);

			var bytes = dts.ToByArray();
			Assert.NotNull(bytes);
			Assert.Equal(20, bytes.Length);

			var back = new SqlDayToSecond(bytes);
			Assert.Equal(dts.Days, back.Days);
			Assert.Equal(dts.Hours, back.Hours);
			Assert.Equal(dts.Minutes, back.Minutes);
			Assert.Equal(dts.Seconds, back.Seconds);
			Assert.Equal(dts.Milliseconds, back.Milliseconds);
		}

		[Theory]
		[InlineData(2, 22, 11, 10, 222, "2.22:11:10.2220000")]
		[InlineData(0, 11, 23, 01, 445, "11:23:01.4450000")]
		public static void GetString(int days, int hours, int minutes, int seconds, int millis, string expected) {
			var dts = new SqlDayToSecond(days, hours, minutes, seconds, millis);

			var s = dts.ToString();
			Assert.Equal(expected, s);
		}

		[Theory]
		[InlineData("02.20:12:55.322", 2, 20, 12, 55, 322)]
		public static void TryParse(string s, int days, int hours, int minutes, int seconds, int millis) {
			SqlDayToSecond dts;
			Assert.True(SqlDayToSecond.TryParse(s, out dts));

			Assert.Equal(days, dts.Days);
			Assert.Equal(hours, dts.Hours);
			Assert.Equal(minutes, dts.Minutes);
			Assert.Equal(seconds, dts.Seconds);
			Assert.Equal(millis, dts.Milliseconds);
		}

		[Theory]
		[InlineData("22 01:00:00.222")]
		[InlineData("")]
		public static void TryInvalidParse(string s) {
			SqlDayToSecond dts;
			Assert.False(SqlDayToSecond.TryParse(s, out dts));
		}

		[Theory]
		[InlineData("")]
		[InlineData("1 32.33:223")]
		public static void ParseInvalid(string s) {
			Assert.Throws<FormatException>(() => SqlDayToSecond.Parse(s));
		}

		[Theory]
		[InlineData(2, 10, 11, 46, 112, "-2.10:11:46.1120000")]
		[InlineData(0, 04, 02, 10, 445, "-04:02:10.4450000")]
		public static void Negate(int days, int hours, int minutes, int seconds, int millis, string expected) {
			var dts = new SqlDayToSecond(days, hours, minutes, seconds, millis);
			var result = -dts;

			Assert.Equal(expected, result.ToString());
		}

		[Theory]
		[InlineData("2.20:11:32", "10:02:30.334", "3.06:14:02.3340000")]
		[InlineData("22:01:10.223", "-02:10:32", "19:50:38.2230000")]
		public static void Add(string dts1, string dts2, string expected) {
			var d1 = SqlDayToSecond.Parse(dts1);
			var d2 = SqlDayToSecond.Parse(dts2);

			var result = d1+ d2;

			var expectedResult = SqlDayToSecond.Parse(expected);
			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData("4.08:12:01.442", "23:22:13.557", "3.08:49:47.8850000")]
		[InlineData("22:01:10.223", "-02:10:32", "1.00:11:42.2230000")]
		public static void Subtract(string dts1, string dts2, string expected) {
			var d1 = SqlDayToSecond.Parse(dts1);
			var d2 = SqlDayToSecond.Parse(dts2);

			var result = d1 - d2;

			var expectedResult = SqlDayToSecond.Parse(expected);
			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData("4.08:12:01.442", "23:22:13.557", false)]
		[InlineData("22:01:10.223", "22:01:10.223", true)]
		public static void Equal(string dts1, string dts2, bool expected) {
			var d1 = SqlDayToSecond.Parse(dts1);
			var d2 = SqlDayToSecond.Parse(dts2);

			var result = d1 == d2;

			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData("3.02:33:23", "2.12:20:02", 1)]
		public static void Compare_ToSqlDayToSecond(string dts1, string dts2, int expected) {
			var d1 = SqlDayToSecond.Parse(dts1);
			var d2 = SqlDayToSecond.Parse(dts2);

			var result = d1.CompareTo(d2);

			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(01, 22, 43, 33, 544)]
		public static void Serialize(int days, int hours, int minutes, int seconds, int millis) {
			var dts = new SqlDayToSecond(days, hours, minutes, seconds, millis);
			var result = BinarySerializeUtil.Serialize(dts);

			Assert.Equal(dts, result);
		}
	}
}