using System;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlNumberTests {
		[Fact]
		public static void Create_FromInteger() {
			var value = (SqlNumber)45993;
			Assert.True(value.CanBeInt32);
			Assert.True(value.CanBeInt64);
			Assert.Equal(0, value.Scale);
			Assert.Equal(5, value.Precision);
			Assert.False(SqlNumber.IsNaN(value));
			Assert.False(SqlNumber.IsNegativeInfinity(value));
			Assert.False(SqlNumber.IsPositiveInfinity(value));
			Assert.Equal(1, value.Sign);
		}

		[Fact]
		public static void Create_FromBigInt() {
			var value = (SqlNumber)4599356655L;
			Assert.False(value.CanBeInt32);
			Assert.True(value.CanBeInt64);
			Assert.Equal(0, value.Scale);
			Assert.Equal(10, value.Precision);
			Assert.False(SqlNumber.IsNaN(value));
			Assert.False(SqlNumber.IsNegativeInfinity(value));
			Assert.False(SqlNumber.IsPositiveInfinity(value));
			Assert.Equal(1, value.Sign);
		}

		[Fact]
		public static void Create_FromDouble() {
			var value = (SqlNumber)459935.9803d;
			Assert.False(value.CanBeInt32);
			Assert.False(value.CanBeInt64);
			Assert.Equal(10, value.Scale);
			Assert.Equal(16, value.Precision);
			Assert.False(SqlNumber.IsNaN(value));
			Assert.False(SqlNumber.IsNegativeInfinity(value));
			Assert.False(SqlNumber.IsPositiveInfinity(value));
			Assert.Equal(1, value.Sign);
		}

		[Fact]
		public static void Parse_BigDecimal() {
			var value = SqlNumber.Parse("98356278.911288837773848500069994933229238e45789");
			Assert.False(value.CanBeInt32);
			Assert.False(value.CanBeInt64);
			Assert.True(value.Precision > 40);
		}

		[Theory]
		[InlineData("98334454", 98334454)]
		[InlineData("6785553.89e3", 6785553.89e3)]
		[InlineData("-435", -435)]
		[InlineData("+Inf", Double.PositiveInfinity)]
		[InlineData("-Inf", Double.NegativeInfinity)]
		[InlineData("NaN", Double.NaN)]
		public static void TryParse(string s, double expected) {
			var expectedResult = (SqlNumber) expected;

			SqlNumber number;
			Assert.True(SqlNumber.TryParse(s, out number));
			Assert.Equal(expectedResult, number);
		}

		[Theory]
		[InlineData("test")]
		[InlineData("")]
		public static void TryInvalidParse(string s) {
			SqlNumber number;
			Assert.False(SqlNumber.TryParse(s, out number));
		}

		[Theory]
		[InlineData(Double.PositiveInfinity, true)]
		[InlineData(67784.00033, false)]
		public static void EqualsToPositiveInfinity(double value, bool expected) {
			var number = (SqlNumber)value;

			Assert.Equal(expected, number.Equals(SqlNumber.PositiveInfinity));
		}

		[Theory]
		[InlineData(Double.PositiveInfinity, Double.PositiveInfinity, 0)]
		[InlineData(Double.PositiveInfinity, 09923, 1)]
		[InlineData(Double.NaN, Double.PositiveInfinity, 1)]
		[InlineData(78333.9122, Double.NegativeInfinity, 1)]
		public static void CompareToInvalidState(double value1, double value2, int expected) {
			var number1 = (SqlNumber)value1;
			var number2 = (SqlNumber)value2;
			Assert.Equal(expected, number1.CompareTo(number2));
		}

		[Theory]
		[InlineData(1, true)]
		[InlineData(0, false)]
		public static void Convert_ToSqlBoolean(int i, bool expected) {
			var value = (SqlNumber)i;
			var result = Convert.ChangeType(value, typeof(SqlBoolean));
			Assert.IsType<SqlBoolean>(result);
			Assert.Equal(expected, (bool)(SqlBoolean)result);
		}

		[Theory]
		[InlineData(-346.76672)]
		[InlineData(543)]
		[InlineData(322.3223e12)]
		public static void Convert_ToByteArray(double value) {
			var number = (SqlNumber)value;
			var result = Convert.ChangeType(number, typeof(byte[]));

			Assert.NotNull(result);
			Assert.IsType<byte[]>(result);

			var resultNumber = new SqlNumber((byte[])result);
			Assert.Equal(number, resultNumber);
		}

		[Theory]
		[InlineData(673884.9033)]
		[InlineData(7448)]
		[InlineData(-02933)]
		public static void Convert_ToSqlBinary(double value) {
			var number = (SqlNumber)value;
			var result = Convert.ChangeType(number, typeof(SqlBinary));

			Assert.NotNull(result);
			Assert.IsType<SqlBinary>(result);

			var bytes = ((SqlBinary) result).ToByteArray();
			var resultNumber = new SqlNumber(bytes);
			Assert.Equal(number, resultNumber);
		}

		[Theory]
		[InlineData(4566748, TypeCode.Int32)]
		[InlineData(67484433323, TypeCode.Int64)]
		[InlineData((byte)22, TypeCode.Byte)]
		[InlineData((short)9330, TypeCode.Int16)]
		[InlineData(9033.443, TypeCode.Double)]
		[InlineData(1220.09f, TypeCode.Single)]
		[InlineData(94055332.6557, TypeCode.Double)]
		public static void Convert_GetTypeCode(object value, TypeCode expected) {
			var number = (SqlNumber) SqlValueUtil.FromObject(value);
			var typeCode = Convert.GetTypeCode(number);
			Assert.Equal(expected, typeCode);
		}

		[Theory]
		[InlineData(1, typeof(bool), true)]
		[InlineData(0, typeof(bool), false)]
		[InlineData(20117, typeof(int), 20117)]
		[InlineData(933.3003, typeof(int), 933)]
		[InlineData(456773838, typeof(long), 456773838L)]
		[InlineData(93562.9112, typeof(long), 93562L)]
		[InlineData(234, typeof(byte), (byte) 234)]
		[InlineData(1, typeof(byte), (byte)1)]
		[InlineData(4578, typeof(short), (short)4578)]
		[InlineData(-6734, typeof(short), (short) -6734)]
		[InlineData(78466373.00091e3, typeof(double), 78466373.00091e3)]
		[InlineData(455.019, typeof(float), 455.019f)]
		public static void Convert_ChangeType(double value, Type type, object expected) {
			var number = (SqlNumber)value;
			var result = Convert.ChangeType(number, type);
			Assert.IsType(type, result);
			Assert.Equal(expected, result);
		}

		[Fact]
		public static void InvalidConvert_SByte()
		{
			InvalidConvertTo(typeof(sbyte));
		}

		[Fact]
		public static void InvalidConvert_UInt32()
		{
			InvalidConvertTo(typeof(uint));
		}

		[Fact]
		public static void InvalidConvert_Char()
		{
			InvalidConvertTo(typeof(char));
		}

		[Fact]
		public static void InvalidConvert_UInt16()
		{
			InvalidConvertTo(typeof(ushort));
		}

		[Fact]
		public static void InvalidConvert_DateTime()
		{
			InvalidConvertTo(typeof(DateTime));
		}


		private static void InvalidConvertTo(Type type) {
			var number = SqlNumber.One;
			Assert.Throws<InvalidCastException>(() => Convert.ChangeType(number, type));
		}


		[Fact]
		public static void BigNumber_Greater_True() {
			var value1 = SqlNumber.Parse("98356278.911288837773848500069994933229238e45789");
			var value2 = SqlNumber.Parse("348299.01991828833333333333488888388829911182227373738488847112349928");

			Assert.True(value1 > value2);
		}

		[Theory]
		[InlineData(34)]
		[InlineData(12784774)]
		public static void Int32_Convert(int value) {
			var number = (SqlNumber)value;

			var result = Convert.ChangeType(number, typeof(int));

			Assert.IsType<int>(result);
			Assert.Equal(value, (int)result);
		}

		[Theory]
		[InlineData(9010)]
		[InlineData(87749948399)]
		public static void Int64_Convert(long value) {
			var number = (SqlNumber)value;

			var result = Convert.ChangeType(number, typeof(long));

			Assert.IsType<long>(result);
			Assert.Equal(value, (long)result);
		}

		[Theory]
		[InlineData(90.121)]
		[InlineData(119299.0029)]
		public static void Double_Convert(double value) {
			var number = (SqlNumber)value;

			var result = Convert.ChangeType(number, typeof(double));

			Assert.IsType<double>(result);
			Assert.Equal(value, (double)result);
		}

		[Theory]
		[InlineData(100)]
		[InlineData(2)]
		public static void Byte_Convert(byte value) {
			var number = (SqlNumber)value;

			var result = Convert.ChangeType(number, typeof(byte));

			Assert.IsType<byte>(result);
			Assert.Equal(value, (byte)result);
		}

		#region Operators

		#region Binary Operators

		private static void BinaryOp(Func<SqlNumber, SqlNumber, SqlNumber> op, double a, double b, double expected) {
			var num1 = (SqlNumber)a;
			var num2 = (SqlNumber)b;

			var result = op(num1, num2);

			Assert.NotNull(result);
			var expectedNumber = (SqlNumber)expected;

			Assert.Equal(expectedNumber, result);
		}

		private static void BinaryOp(Func<SqlNumber, SqlNumber, SqlBoolean> op, double a, double b, bool expected) {
			var num1 = (SqlNumber)a;
			var num2 = (SqlNumber)b;

			var result = op(num1, num2);

			Assert.NotNull(result);
			var expectedResult = (SqlBoolean)expected;

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(4533, 90, 33)]
		[InlineData(Double.NaN, 90332, Double.NaN)]
		public static void Operator_Modulo(double value1, double value2, double expected) {
			BinaryOp((x, y) => x % y, value1, value2, expected);
		}

		[Theory]
		[InlineData(466637, 9993, 476630)]
		[InlineData(7833.432, -23, 7810.432)]
		[InlineData(0933.42, Double.NegativeInfinity, Double.NegativeInfinity)]
		[InlineData(Double.PositiveInfinity, 344, Double.PositiveInfinity)]
		public static void Operator_Add(double value1, double value2, double expected) {
			BinaryOp((x, y) => x + y, value1, value2, expected);
		}

		[Theory]
		[InlineData(5455261, 119020, 5336241)]
		[InlineData(-45563, 453.332, -46016.332)]
		[InlineData(-5433, Double.PositiveInfinity, Double.NegativeInfinity)]
		[InlineData(Double.PositiveInfinity, Double.NegativeInfinity, Double.PositiveInfinity)]
		public static void Operator_Subtract(double value1, double value2, double expected) {
			BinaryOp((x, y) => x - y, value1, value2, expected);
		}

		[Theory]
		[InlineData(2783, 231, 642873)]
		[InlineData(-9032.654, -45, 406469.43)]
		[InlineData(434.22, Double.PositiveInfinity, Double.PositiveInfinity)]
		public static void Operator_Multiply(double value1, double value2, double expected) {
			BinaryOp((x, y) => x * y, value1, value2, expected);
		}

		[Theory]
		[InlineData(1152663, 9929, 116.0905)]
		[InlineData(40, 5, 8)]
		public static void Operator_Divide(double value1, double value2, double expected) {
			BinaryOp((x, y) => x / y, value1, value2, expected);
		}

		[Theory]
		[InlineData(5663.22)]
		public static void Operator_DivideByZero(double value) {
			var number = (SqlNumber) value;

			Assert.Throws<DivideByZeroException>(() => number / SqlNumber.Zero);
		}

		[Theory]
		[InlineData(6532, 112, 0)]
		[InlineData(Double.NaN, 2003, Double.NaN)]
		[InlineData(4355, Double.PositiveInfinity, Double.PositiveInfinity)]
		[InlineData(Double.NaN, Double.NaN, Double.NaN)]
		public static void Operator_And(double value1, double value2, double expected) {
			BinaryOp((x, y) => x & y, value1, value2, expected);
		}

		[Theory]
		[InlineData(46677, 9982, false)]
		[InlineData(92677, 92677, true)]
		public static void Operator_Equal(double value1, double value2, bool expected) {
			BinaryOp((x, y) => x == y, value1, value2, expected);
		}

		[Theory]
		[InlineData(46677, 9982, true)]
		[InlineData(92677, 92677, false)]
		public static void Operator_NotEqual(double value1, double value2, bool expected) {
			BinaryOp((x, y) => x != y, value1, value2, expected);
		}

		[Theory]
		[InlineData(4785, 112, 4849)]
		[InlineData(6748, Double.PositiveInfinity, Double.PositiveInfinity)]
		public static void Operator_Or(double value1, double value2, double expected) {
			BinaryOp((x, y) => x|y, value1, value2, expected);
		}

		[Theory]
		[InlineData(73844, 13, 73849)]
		[InlineData(566, Double.NaN, Double.NaN)]
		public static void Operator_XOr(double value1, double value2, double expected) {
			BinaryOp((x, y) => x ^ y, value1, value2, expected);
		}

		[Theory]
		[InlineData(2133, 123, true)]
		[InlineData(65484.213e21, 54331432.546e121, false)]
		public static void Operator_Greater(double value1, double value2, bool expected) {
			BinaryOp((x, y) => x > y, value1, value2, expected);
		}

		[Theory]
		[InlineData(546649, 2112, true)]
		[InlineData(4333.678, 4333.678, true)]
		[InlineData(93445, 1200.345e32, false)]
		public static void Operator_GreaterOrEqual(double value1, double value2, bool expected) {
			BinaryOp((x, y) => x >= y, value1, value2, expected);
		} 

		[Theory]
		[InlineData(7484, 1230449, true)]
		[InlineData(102943e45, 201e12, false)]
		public static void Operator_Smaller(double value1, double value2, bool expected) {
			BinaryOp((x, y) => x < y, value1, value2, expected);
		}

		[Theory]
		[InlineData(893.33, 322.45e23, true)]
		[InlineData(2331, 15e3, true)]
		[InlineData(901.54e123, 901.54e123, true)]
		[InlineData(9120, 102, false)]
		public static void Operator_SmallerOrEqual(double value1, double value2, bool expected) {
			BinaryOp((x, y) => x <= y, value1, value2, expected);
		}

		#endregion

		private static void UnaryOp(Func<SqlNumber, SqlNumber> op, double value, double expected) {
			var number = (SqlNumber)value;

			var result = op(number);
			var expectedResult = (SqlNumber) expected;

			Assert.NotNull(result);
			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(7782, -7782)]
		[InlineData(-9021, 9021)]
		public static void Operator_Negate(double value, double expected) {
			UnaryOp(x => -x, value, expected);
		}

		[Theory]
		[InlineData(7782, 7782)]
		[InlineData(-9021, -9021)]
		public static void Operator_Plus(double value, double expected) {
			UnaryOp(x => +x, value, expected);
		}

		[Theory]
		[InlineData(4599, -4600)]
		[InlineData(-5633, 5632)]
		public static void Operator_Not(double value, double expected) {
			UnaryOp(x => ~x, value, expected);
		}

		#endregion

		[Theory]
		[InlineData(455.331, "455.331")]
		[InlineData(67, "67")]
		[InlineData(126477489, "126477489")]
		[InlineData(-67488493, "-67488493")]
		[InlineData(Double.NaN, "NaN")]
		[InlineData(Double.PositiveInfinity, "+Infinity")]
		[InlineData(Double.NegativeInfinity, "-Infinity")]
		public static void GetString(double value, string expected) {
			var number = (SqlNumber)value;
			var s = number.ToString();
			Assert.Equal(expected, s);
		}
	}
}