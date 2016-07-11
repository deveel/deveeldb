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
	[Category("Numbers")]
	public class SqlNumberTests {
		[Test]
		public void Create_FromInteger() {
			var value = new SqlNumber((int) 45993);
			Assert.IsFalse(value.IsNull);
			Assert.IsTrue(value.CanBeInt32);
			Assert.IsTrue(value.CanBeInt64);
			Assert.AreEqual(0, value.Scale);
			Assert.AreEqual(5, value.Precision);
			Assert.AreEqual(NumericState.None, value.State);
			Assert.AreEqual(1, value.Sign);
		}

		[Test]
		public void Create_FromBigInt() {
			var value = new SqlNumber(4599356655L);
			Assert.IsFalse(value.IsNull);
			Assert.IsFalse(value.CanBeInt32);
			Assert.IsTrue(value.CanBeInt64);
			Assert.AreEqual(0, value.Scale);
			Assert.AreEqual(10, value.Precision);
			Assert.AreEqual(NumericState.None, value.State);
			Assert.AreEqual(1, value.Sign);			
		}

		[Test]
		public void Create_FromDouble() {
			var value = new SqlNumber(459935.9803d);
			Assert.IsFalse(value.IsNull);
			Assert.IsFalse(value.CanBeInt32);
			Assert.IsFalse(value.CanBeInt64);
			Assert.AreEqual(28, value.Scale);
			Assert.AreEqual(34, value.Precision);
			Assert.AreEqual(NumericState.None, value.State);
			Assert.AreEqual(1, value.Sign);
		}

		[Test]
		public void Parse_BigDecimal() {
			var value = new SqlNumber();
			Assert.DoesNotThrow(() => value = SqlNumber.Parse("98356278.911288837773848500069994933229238e45789"));
			Assert.IsFalse(value.IsNull);
			Assert.IsFalse(value.CanBeInt32);
			Assert.IsFalse(value.CanBeInt64);
			Assert.Greater(value.Precision, 40);
		}

		[Test]
		public void Convert_ToBoolean_Success() {
			var value = SqlNumber.One;
			var b = new SqlBoolean();
			Assert.DoesNotThrow(() => b = (SqlBoolean)Convert.ChangeType(value, typeof(SqlBoolean)));
			Assert.IsTrue(b);
		}

		[Test]
		public void Integer_Greater_True() {
			var value1 = new SqlNumber(76);
			var value2 = new SqlNumber(54);

			Assert.IsTrue(value1 > value2);
		}

		[Test]
		public static void BigNumber_Greater_True() {
			var value1 = SqlNumber.Parse("98356278.911288837773848500069994933229238e45789");
			var value2 = SqlNumber.Parse("348299.01991828833333333333488888388829911182227373738488847112349928");

			Assert.IsTrue(value1 > value2);
		}

		[Category("Conversion"), Category("Numbers")]
		[TestCase(34)]
		[TestCase(12784774)]
		public static void Int32_Convert(int value) {
			var number = new SqlNumber(value);

			var result = Convert.ChangeType(number, typeof(int));

			Assert.IsInstanceOf<int>(result);
			Assert.AreEqual(value, (int)result);
		}

		[Category("Conversion"), Category("Numbers")]
		[TestCase(9010)]
		[TestCase(87749948399)]
		public static void Int64_Convert(long value) {
			var number = new SqlNumber(value);

			var result = Convert.ChangeType(number, typeof(long));

			Assert.IsInstanceOf<long>(result);
			Assert.AreEqual(value, (long)result);
		}

		[Category("Conversion"), Category("Numbers")]
		[TestCase(90.121)]
		[TestCase(119299.0029)]
		public static void Double_Convert(double value) {
			var number = new SqlNumber(value);

			var result = Convert.ChangeType(number, typeof(double));

			Assert.IsInstanceOf<double>(result);
			Assert.AreEqual(value, (double)result);
		}

		[Category("Conversion"), Category("Numbers")]
		[TestCase(100)]
		[TestCase(2)]
		public static void Byte_Convert(byte value) {
			var number = new SqlNumber(value);

			var result = Convert.ChangeType(number, typeof(byte));

			Assert.IsInstanceOf<byte>(result);
			Assert.AreEqual(value, (byte)result);
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(466637, 9993, 476630)]
		public static void Operator_Add(int value1, int value2, int expected) {
			var num1 = new SqlNumber(value1);
			var num2 = new SqlNumber(value2);

			var result = num1 + num2;

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsTrue(result.CanBeInt32);

			var intResult = result.ToInt32();

			Assert.AreEqual(expected, intResult);
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(5455261, 119020, 5336241)]
		public static void Operator_Subtract(int value1, int value2, int expected) {
			var num1 = new SqlNumber(value1);
			var num2 = new SqlNumber(value2);

			var result = num1 - num2;

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsTrue(result.CanBeInt32);

			var intResult = result.ToInt32();

			Assert.AreEqual(expected, intResult);
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(2783, 231, 642873)]
		public static void Operator_Multiply(int value1, int value2, int expected) {
			var num1 = new SqlNumber(value1);
			var num2 = new SqlNumber(value2);

			var result = num1 * num2;

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsTrue(result.CanBeInt32);

			var intResult = result.ToInt32();

			Assert.AreEqual(expected, intResult);
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(1152663, 9929, 116.0905428543)]
		[TestCase(40, 5, 8)]
		public static void Operator_Divide(int value1, int value2, double expected) {
			var num1 = new SqlNumber(value1);
			var num2 = new SqlNumber(value2);

			var result = num1 / num2;

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			var doubleResult = result.ToDouble();

			Assert.AreEqual(expected, doubleResult);
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(7782, -7782)]
		[TestCase(-9021, 9021)]
		public static void Operator_Negate(int value, int expected) {
			var number = new SqlNumber(value);

			var result = -number;

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsTrue(result.CanBeInt32);

			Assert.AreEqual(expected, result.ToInt32());
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(7782, 7782)]
		[TestCase(-9021, -9021)]
		public static void Operator_Plus(int value, int expected) {
			var number = new SqlNumber(value);

			var result = +number;

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsTrue(result.CanBeInt32);

			Assert.AreEqual(expected, result.ToInt32());
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(46677, 9982, false)]
		[TestCase(92677, 92677, true)]
		public static void Operator_Int32Equal(int value1, int value2, bool expected) {
			var num1 = new SqlNumber(value1);
			var num2 = new SqlNumber(value2);

			var result = num1 == num2;

			Assert.AreEqual(expected, result);
		}

		[Test]
		public static void Operator_EqualToNull() {
			var number = new SqlNumber(563663.9920);

			var result = number == null;

			Assert.AreEqual(false, result);
		}

		[Category("Numbers"), Category("Operators")]
		[TestCase(46677, 9982, true)]
		[TestCase(92677, 92677, false)]
		public static void Operator_Int32NotEqual(int value1, int value2, bool expected) {
			var num1 = new SqlNumber(value1);
			var num2 = new SqlNumber(value2);

			var result = num1 != num2;

			Assert.AreEqual(expected, result);
		}

		[Category("Numbers"), Category("Functions")]
		[TestCase(455, 3, 94196375)]
		public static void Function_Pow(int value, int exp, double expected) {
			var number = new SqlNumber(value);
			var result = number.Pow(new SqlNumber(exp));

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			var doubleResult = result.ToDouble();

			Assert.AreEqual(expected, doubleResult);
		}

		[Category("Numbers"), Category("Functions")]
		[TestCase(99820, 48993, 1.0659007887179623)]
		public static void Function_Log(int value, int newBase, double expected) {
			var number = new SqlNumber(value);
			var result = number.Log(new SqlNumber(newBase));

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			var doubleResult = result.ToDouble();

			Assert.AreEqual(expected, doubleResult);
		}

		[Category("Numbers"), Category("Functions")]
		[TestCase(9963, -0.53211858514845722)]
		public static void Function_Cos(int value, double expected) {
			var number = new SqlNumber(value);
			var result = number.Cos();

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			var doubleResult = result.ToDouble();

			Assert.AreEqual(expected, doubleResult);
		}

		[Category("Numbers"), Category("Functions")]
		[TestCase(0.36f, 1.0655028755774867)]
		public static void Function_CosH(float value, double expected) {
			var number = new SqlNumber(value);
			var result = number.CosH();

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			var doubleResult = result.ToDouble();

			Assert.AreEqual(expected, doubleResult);
		}
	}
}