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

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlYearToMonthTests {
		//[Theory]
		//[InlineData(2, 11)]
		//[InlineData(0, 8)]
		//public static void Serialize(int years, int months) {
		//	var ytm = new SqlYearToMonth(years, months);
		//	var result = BinarySerializeUtil.Serialize(ytm);

		//	Assert.Equal(result, ytm);
		//}

		[Theory]
		[InlineData(3, 36)]
		[InlineData(1, 12)]
		public static void FromYear(int years, int expectedMonths) {
			var ytm = new SqlYearToMonth(years, 0);

			Assert.NotNull(ytm);
			Assert.Equal(expectedMonths, ytm.TotalMonths);
			Assert.Equal(years, ytm.TotalYears);
		}

		[Theory]
		[InlineData(34, 2.8333333333333335)]
		[InlineData(24, 2)]
		public static void FromMonths(int months, double expectedYears) {
			var ytm = new SqlYearToMonth(months);

			Assert.NotNull(ytm);
			Assert.Equal(expectedYears, ytm.TotalYears);
			Assert.Equal(months, ytm.TotalMonths);
			;
		}

		[Theory]
		[InlineData(1, 12, 24)]
		[InlineData(2, 0, 24)]
		[InlineData(-2, 0, -24)]
		public static void FromYearsAndMonths(int years, int months, int expectedMonths) {
			var ytm = new SqlYearToMonth(years, months);

			Assert.NotNull(ytm);
			Assert.Equal(expectedMonths, ytm.TotalMonths);
		}

		[Theory]
		[InlineData(56, 56, true)]
		[InlineData(23, 22, false)]
		public static void EqualToOther(int months1, int months2, bool expected) {
			var ytm1 = new SqlYearToMonth(months1);
			var ytm2 = new SqlYearToMonth(months2);

			Assert.Equal(expected, ytm1.Equals(ytm2));
		}

		[Theory]
		[InlineData(65, 65, 0)]
		[InlineData(21, 65, -1)]
		[InlineData(11, 21, -1)]
		[InlineData(33, 10, 1)]
		public static void Compare_ToNumber(int months, int value, int expected) {
			var ytm = new SqlYearToMonth(months);
			var number = (SqlNumber) value;

			var result = ytm.CompareTo(number);
			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(32, 43, -1)]
		[InlineData(22, 1, 1)]
		[InlineData(3, 3, 0)]
		public static void Compare_ToYearToMonth(int months1, int months2, int expected) {
			var ytm1 = new SqlYearToMonth(months1);
			var ytm2 = new SqlYearToMonth(months2);

			var result = ytm1.CompareTo(ytm2);
			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(15, 32, -1)]
		public static void Compare_ToSqlValue_Number(int months, int value, int expected) {
			var ytm = new SqlYearToMonth(months);
			var number = (SqlNumber) value;

			var result = (ytm as IComparable<ISqlValue>).CompareTo(number);
			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(1, 2, -1)]
		[InlineData(54, 33, 1)]
		public static void Compare_ToSqlValue_YearToMonth(int months1, int months2, int expected) {
			var ytm1 = new SqlYearToMonth(months1);
			var ytm2 = new SqlYearToMonth(months2);

			var result = (ytm1 as IComparable<ISqlValue>).CompareTo(ytm2);
			Assert.Equal(expected, result);
		}


		[Fact]
		public static void InvalidComparison() {
			var ytm = new SqlYearToMonth(2);
			Assert.Throws<NotSupportedException>(() => (ytm as IComparable<ISqlValue>).CompareTo(SqlBoolean.True));
		}

		[Theory]
		[InlineData(23, 56, false)]
		[InlineData(22, 22, true)]
		public static void Equal(int value1, int value2, bool expected) {
			BinaryOp((x, y) => x == y, value1, value2, expected);
		}

		[Theory]
		[InlineData(1, 2, true)]
		[InlineData(45, 45, false)]
		public static void NotEqual(int value1, int value2, bool expected) {
			BinaryOp((x, y) => x != y, value1, value2, expected);
		}


		[Theory]
		[InlineData(3, 4, false)]
		[InlineData(5, 2, true)]
		public static void Greater(int value1, int value2, bool expected) {
			BinaryOp((x, y) => x > y, value1, value2, expected);
		}


		[Theory]
		[InlineData(3, 4, true)]
		[InlineData(5, 2, false)]
		public static void Smaller(int value1, int value2, bool expected) {
			BinaryOp((x, y) => x < y, value1, value2, expected);
		}


		[Theory]
		[InlineData(3, 4, false)]
		[InlineData(5, 5, true)]
		[InlineData(5, 2, true)]
		public static void GreaterOrEqual(int value1, int value2, bool expected) {
			BinaryOp((x, y) => x >= y, value1, value2, expected);
		}


		[Theory]
		[InlineData(3, 4, true)]
		[InlineData(7, 7, true)]
		[InlineData(5, 2, false)]
		public static void SmallerOrEqual(int value1, int value2, bool expected) {
			BinaryOp((x, y) => x <= y, value1, value2, expected);
		}

		[Theory]
		[InlineData(4, 4, 8)]
		[InlineData(4, 2, 6)]
		public static void AddMonths(int value1, int value2, int expected) {
			BinaryOp((x, y) => x + y, value1, value2, expected);
		}

		[Theory]
		[InlineData(4, 2, 2)]
		[InlineData(4, 3, 1)]
		public static void SubtractMonths(int value1, int value2, int expected) {
			BinaryOp((x, y) => x - y, value1, value2, expected);
		}

		private static void BinaryOp(Func<SqlYearToMonth, SqlYearToMonth, bool> op,
			int months1,
			int months2,
			bool expected) {
			var ytm1 = (SqlYearToMonth) months1;
			var ytm2 = (SqlYearToMonth) months2;

			var result = op(ytm1, ytm2);

			Assert.Equal(expected, result);
		}

		public static void BinaryOp(Func<SqlYearToMonth, SqlYearToMonth, SqlYearToMonth> op,
			int months1,
			int months2,
			int expected) {
			var ytm1 = (SqlYearToMonth)months1;
			var ytm2 = (SqlYearToMonth)months2;

			var result = op(ytm1, ytm2);

			var expectedResult = (SqlYearToMonth) expected;
			Assert.Equal(expectedResult, result);
		}
	}
}