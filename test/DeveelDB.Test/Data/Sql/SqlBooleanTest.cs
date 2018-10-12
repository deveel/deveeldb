using System;
using System.ComponentModel;
using System.Globalization;

using Xunit;

namespace Deveel.Data.Sql {
	public class SqlBooleanTest {
		[Theory]
		[InlineData(1, true)]
		[InlineData(0, false)]
		public void CreateFromByte(byte value, bool expected) {
			var b = new SqlBoolean(value);
			var expectedResult = (SqlBoolean) expected;
			Assert.Equal(expectedResult,b);
		}

		[Theory]
		[InlineData(true)]
		[InlineData(false)]
		public void CreateFromBoolean(bool value) {
			var b = (SqlBoolean) value;
			Assert.NotNull(b);
			Assert.Equal(value, (bool)b);
		}

		[Theory]
		[InlineData(true, true, 0)]
		[InlineData(false, true, -1)]
		[InlineData(false, false, 0)]
		[InlineData(true, 1, 0)]
		[InlineData(false, 0, 0)]
		[InlineData(true, 0, 1)]
		public void Compare(bool value1, object value2, int expected) {
			var a = (SqlBoolean) value1;
			var b = SqlValueUtil.FromObject(value2);

			Assert.True((a as ISqlValue).IsComparableTo(b));

			var result = a.CompareTo(b);
			Assert.Equal(expected, result);
		}

		[Fact]
		public void Compare_ToNull() {
			var value1 = SqlBoolean.True;
			var value2 = SqlNull.Value;

			Assert.NotNull(value1);

			Assert.False((value1 as ISqlValue).IsComparableTo(value2));
			Assert.Throws<ArgumentException>(() => value1.CompareTo(value2));
		}

		[Fact]
		public void Compare_ToNumber_OutOfRange() {
			var value1 = SqlBoolean.True;
			var value2 = (SqlNumber)21;

			Assert.NotNull(value1);
			Assert.NotNull(value2);

			Assert.False((value1 as ISqlValue).IsComparableTo(value2));

			int i = -2;
			Assert.Throws<ArgumentOutOfRangeException>(() => i = value1.CompareTo(value2));
			Assert.Equal(-2, i);
		}

		[Theory]
		[InlineData(true, true, true)]
		[InlineData(true, false, false)]
		[InlineData(false, false, true)]
		public void Equal(bool value1, bool value2, bool expected) {
			BinaryOp((x, y) => x == y, value1, value2, expected);
		}

		private static void BinaryOp(Func<SqlBoolean, SqlBoolean, SqlBoolean> op, bool value1, bool value2, bool expected) {
			OperatorsUtil.Binary((x, y) => op((SqlBoolean)x, (SqlBoolean)y), value1, value2, expected);
		}

		[Theory]
		[InlineData(true, typeof(bool), true)]
		[InlineData(false, typeof(bool), false)]
		[InlineData(true, typeof(string), "true")]
		[InlineData(false, typeof(string), "false")]
		[InlineData(true, typeof(int), 1)]
		[InlineData(false, typeof(int), 0)]
		[InlineData(true, typeof(short), (short)1)]
		[InlineData(false, typeof(short), (short) 0)]
		[InlineData(true, typeof(long), 1L)]
		[InlineData(false, typeof(long), 0L)]
		[InlineData(true, typeof(float), 1f)]
		[InlineData(false, typeof(float), 0f)]
		[InlineData(true, typeof(double), 1d)]
		[InlineData(false, typeof(double), 0d)]
		[InlineData(true, typeof(uint), (uint)1)]
		[InlineData(false, typeof(uint), (uint)0)]
		[InlineData(true, typeof(ushort), (ushort)1)]
		[InlineData(false, typeof(ushort), (ushort)0)]
		[InlineData(true, typeof(ulong), (ulong)1)]
		[InlineData(false, typeof(ulong), (ulong)0)]
		[InlineData(true, typeof(byte), (byte)1)]
		[InlineData(false, typeof(byte), (byte)0)]
		[InlineData(true, typeof(sbyte), (sbyte)1)]
		[InlineData(false, typeof(sbyte), (sbyte)0)]
		public void ConvertValid(bool value, Type destTpe, object expected) {
			var b = (SqlBoolean) value;
			var result = Convert.ChangeType(b, destTpe, CultureInfo.InvariantCulture);

			Assert.NotNull(result);
			Assert.IsType(destTpe, result);
			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(true, typeof(DateTime))]
		[InlineData(false, typeof(DateTime))]
		[InlineData(true, typeof(char))]
		[InlineData(false, typeof(char))]
		public void ConvertInvalid(bool value, Type destType) {
			var b = (SqlBoolean) value;
			Assert.Throws<InvalidCastException>(() => Convert.ChangeType(b, destType, CultureInfo.InvariantCulture));
		}

		[Theory]
		[InlineData(true, 1)]
		[InlineData(false, 0)]
		public static void ConvertToSqlNumber(bool value, int expected) {
			var number = (SqlNumber) expected;
			var b = (SqlBoolean) value;

			var result = Convert.ChangeType(b, typeof(SqlNumber));
			Assert.Equal(number, result);
		}

		[Theory]
		[InlineData(true, true, false)]
		[InlineData(true, false, true)]
		[InlineData(false, false, false)]
		public void XOr(bool b1, bool b2, bool expected) {
			BinaryOp((x, y) => x ^ y, b1, b2, expected);
		}

		[Theory]
		[InlineData(true, true, true)]
		[InlineData(true, false, true)]
		[InlineData(false, false, false)]
		public void Or(bool b1, bool b2, bool expected) {
			BinaryOp((x, y) => x | y, b1, b2, expected);
		}

		[Theory]
		[InlineData("true", true)]
		[InlineData("TRUE", true)]
		[InlineData("TrUe", true)]
		[InlineData("FALSE", false)]
		[InlineData("false", false)]
		[InlineData("FaLsE", false)]
		[InlineData("1", true)]
		[InlineData("0", false)]
		public void Parse(string s, bool expected) {
			var result = SqlBoolean.Parse(s);

			Assert.Equal((SqlBoolean) expected, result);
		}

		[Theory]
		[InlineData("true", true, true)]
		[InlineData("TRUE", true, true)]
		[InlineData("TrUe", true, true)]
		[InlineData("FALSE", false, true)]
		[InlineData("false", false, true)]
		[InlineData("FaLsE", false, true)]
		[InlineData("1", true, true)]
		[InlineData("0", false, true)]
		[InlineData("", false, false)]
		[InlineData("445", false, false)]
		[InlineData("t rue", false, false)]
		public void TryParse(string s, bool expected, bool success) {
			SqlBoolean value;
			Assert.Equal(success, SqlBoolean.TryParse(s, out value));
			Assert.Equal(expected, (bool) value);
		}
	}
}