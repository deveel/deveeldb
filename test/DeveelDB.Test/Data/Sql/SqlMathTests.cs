using System;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlMathTests {
		[Theory]
		[InlineData(455, 3, 94196375)]
		public static void Function_Pow(int value, int exp, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Pow(number, (SqlNumber)exp);

			Assert.NotNull(result);

			var doubleResult = (double)result;

			Assert.Equal(expected, doubleResult);
		}

		[Theory]
		[InlineData(99820, 48993, 1.065900788717962)]
		public static void Function_Log(int value, int newBase, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Log(number, (SqlNumber)newBase);

			Assert.NotNull(result);

			var expectedResult = (SqlNumber) expected;

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(9963, -0.53211858514845722)]
		public static void Function_Cos(int value, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Cos(number);

			Assert.NotNull(result);

			var doubleResult = (double)result;

			Assert.Equal(expected, doubleResult);
		}

		[Theory]
		[InlineData(0.36f, 1.065502870315686)]
		public static void Function_CosH(float value, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.CosH(number);

			Assert.NotNull(result);

			var expectedResult = (SqlNumber)expected;

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(-45636.0003922, 45636.0003922)]
		public static void Function_Abs(double value, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Abs(number);

			Assert.NotNull(result);

			var doubleResult = (double)result;

			Assert.Equal(expected, doubleResult);
		}

		[Theory]
		[InlineData(559604.003100, 23.62526523010039)]
		public static void Function_Tan(double value, double expected) {
			var number =(SqlNumber)value;
			var result = SqlMath.Tan(number);

			Assert.NotNull(result);

			var expectedResult = (SqlNumber) expected;

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(89366647.992, 1)]
		public static void Function_TanH(double value, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.TanH(number);

			Assert.NotNull(result);

			var doubleResult = (double)result;

			Assert.Equal(expected, doubleResult);
		}

		[Theory]
		[InlineData(929928.00111992934, 929928.0011199294)]
		public static void Function_Round(double value, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Round(number);

			Assert.NotNull(result);

			var expectedNumber = (SqlNumber)expected;
			Assert.Equal(expectedNumber, result);
		}

		[Theory]
		[InlineData(929928.00111992934, 10, 929928.0011)]
		public static void Function_RoundWithPrecision(double value, int precision, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Round(number, precision);

			Assert.NotNull(result);

			var doubleResult = (double)result;

			Assert.Equal(expected, doubleResult);
		}

		[Theory]
		[InlineData(02993011.338, -0.3040696985546506)]
		public static void Function_Sin(double value, double expected) {
			var number = (SqlNumber)value;
			var result = SqlMath.Sin(number);

			Assert.NotNull(result);

			var expectedResult = (SqlNumber)expected;

			Assert.Equal(expectedResult, result);
		}
	}
}