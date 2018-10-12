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

using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlNullTests {
		[Theory]
		[InlineData(null, true)]
		[InlineData(563543.9921, false)]
		public static void Equal(object other, bool expected) {
			BinaryOp((x, y)=> x == y, other, expected);
		}

		[Theory]
		[InlineData("the quick brown fox", true)]
		[InlineData(null, false)]
		public static void NotEqual(object other, bool expected) {
			BinaryOp((x, y) => x != y, other, expected);
		}

		private static void BinaryOp(Func<SqlNull, ISqlValue, bool> op, object other, bool expected) {
			var null1 = SqlNull.Value;
			var value1 = SqlValueUtil.FromObject(other);

			var result = op(null1, value1);

			var b = (SqlBoolean) result;
			var expectedNumber = (SqlBoolean)expected;

			Assert.Equal(expectedNumber, b);
		}

		[Fact]
		public static void GetString() {
			var sqlNull = SqlNull.Value;
			Assert.Equal("NULL", sqlNull.ToString());
		}

		[Theory]
		[InlineData(typeof(byte))]
		[InlineData(typeof(sbyte))]
		[InlineData(typeof(int))]
		[InlineData(typeof(long))]
		[InlineData(typeof(short))]
		[InlineData(typeof(float))]
		[InlineData(typeof(double))]
		[InlineData(typeof(decimal))]
		[InlineData(typeof(uint))]
		[InlineData(typeof(ushort))]
		[InlineData(typeof(ulong))]
		[InlineData(typeof(bool))]
		[InlineData(typeof(DateTime))]
		[InlineData(typeof(char))]
		public static void InvalidConvertTo(Type type) {
			Assert.Throws<InvalidCastException>(() => Convert.ChangeType(SqlNull.Value, type));
		}

		[Theory]
		[InlineData(typeof(string))]
		[InlineData(typeof(SqlNull))]
		[InlineData(typeof(SqlType))]
		public static void ConvertTo(Type type) {
			var result = Convert.ChangeType(SqlNull.Value, type);
			Assert.Null(result);
		}

		[Fact]
		public static void GetTypeCode() {
			Assert.Equal(TypeCode.Empty, (SqlNull.Value as IConvertible).GetTypeCode());
		}

		[Theory]
		[InlineData(null, 0)]
		[InlineData(true, -1)]
		public static void CompareTo(object other, int expected) {
			var value = SqlValueUtil.FromObject(other);
			Assert.Equal(expected, (SqlNull.Value as IComparable<ISqlValue>).CompareTo(value));
		}

		[Theory]
		[InlineData(null, true)]
		[InlineData(54495.33, false)]
		public static void IsComparableTo(object other, bool expected) {
			var value = SqlValueUtil.FromObject(other);
			Assert.Equal(expected, (SqlNull.Value as ISqlValue).IsComparableTo(value));
		}
	}
}