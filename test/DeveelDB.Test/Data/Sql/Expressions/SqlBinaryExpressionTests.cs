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

namespace Deveel.Data.Sql.Expressions {
	public static class SqlBinaryExpressionTests {
		[Theory]
		[InlineData(SqlExpressionType.Equal, 6577.494, 449.004)]
		[InlineData(SqlExpressionType.Add, 323, 12)]
		public static void CreateBinary(SqlExpressionType expressionType, object value1, object value2) {
			var obj1 = SqlObject.New(SqlValueUtil.FromObject(value1));
			var obj2 = SqlObject.New(SqlValueUtil.FromObject(value2));

			var exp = SqlExpression.Binary(expressionType, SqlExpression.Constant(obj1), SqlExpression.Constant(obj2));

			Assert.NotNull(exp);
			Assert.NotNull(exp.Left);
			Assert.NotNull(exp.Right);
		}

		[Theory]
		[InlineData(2201.112, 203, "2201.112 + 203")]
		public static void GetAddString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Add, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 - 2293.1102")]
		public static void GetSubtractString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Subtract, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 / 2293.1102")]
		public static void GetDivideString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Divide, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 * 2293.1102")]
		public static void GetMultiplyString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Multiply, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 % 2293.1102")]
		public static void GetModuloString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Modulo, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 = 2293.1102")]
		public static void GetEqualString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Equal, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 <> 2293.1102")]
		public static void GetNotEqualString(object value1, object value2, string expected) {
			AssertString(SqlExpression.NotEqual, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 > 2293.1102")]
		public static void GetGreaterThanString(object value1, object value2, string expected) {
			AssertString(SqlExpression.GreaterThan, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 < 2293.1102")]
		public static void GetLessThanString(object value1, object value2, string expected) {
			AssertString(SqlExpression.LessThan, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 >= 2293.1102")]
		public static void GetGreaterThanOrEqualString(object value1, object value2, string expected) {
			AssertString(SqlExpression.GreaterThanOrEqual, value1, value2, expected);
		}

		[Theory]
		[InlineData(12, 2293.1102, "12 <= 2293.1102")]
		public static void GetLessThanOrEqualString(object value1, object value2, string expected) {
			AssertString(SqlExpression.LessThanOrEqual, value1, value2, expected);
		}

		[Theory]
		[InlineData(true, false, "TRUE AND FALSE")]
		public static void GetAndString(object value1, object value2, string expected) {
			AssertString(SqlExpression.And, value1, value2, expected);
		}

		[Theory]
		[InlineData(true, false, "TRUE OR FALSE")]
		public static void GetOrString(object value1, object value2, string expected) {
			AssertString(SqlExpression.Or, value1, value2, expected);
		}

		private static SqlBinaryExpression BinaryExpression(Func<SqlExpression, SqlExpression, SqlBinaryExpression> factory,
			object value1, object value2) {
			var left = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var right = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value2)));

			return factory(left, right);
		}

		private static void AssertString(Func<SqlExpression, SqlExpression, SqlBinaryExpression> factory,
			object value1, object value2, string expected) {
			var exp = BinaryExpression(factory, value1, value2);
			var sql = exp.ToString();
			Assert.Equal(expected, sql);
		}

		[Theory]
		[InlineData(SqlExpressionType.Equal, 34, 34, true)]
		[InlineData(SqlExpressionType.NotEqual, 190, 21, true)]
		[InlineData(SqlExpressionType.GreaterThan, 12.02e32, 23.98, true)]
		[InlineData(SqlExpressionType.GreaterThanOrEqual, 110, 20, true)]
		[InlineData(SqlExpressionType.LessThan, 67, 98, true)]
		[InlineData(SqlExpressionType.LessThanOrEqual, "abc1234", "abc12345", false)]
		[InlineData(SqlExpressionType.Add, 45, 45, 90)]
		[InlineData(SqlExpressionType.Subtract, 102, 30, 72)]
		[InlineData(SqlExpressionType.Multiply, 22, 2, 44)]
		[InlineData(SqlExpressionType.Divide, 100, 2, 50)]
		[InlineData(SqlExpressionType.Is, true, true, true)]
		[InlineData(SqlExpressionType.IsNot, 22.09, false, true)]
		[InlineData(SqlExpressionType.Or, true, false, true)]
		[InlineData(SqlExpressionType.XOr, 113, 56, 73)]
		[InlineData(SqlExpressionType.And, true, false, false)]
		public static async void ReduceBinary(SqlExpressionType expressionType, object value1, object value2, object expected) {
			var obj1 = SqlObject.New(SqlValueUtil.FromObject(value1));
			var obj2 = SqlObject.New(SqlValueUtil.FromObject(value2));

			var exp = SqlExpression.Binary(expressionType, SqlExpression.Constant(obj1), SqlExpression.Constant(obj2));

			Assert.True(exp.CanReduce);

			var reduced = await exp.ReduceAsync(null);

			Assert.NotNull(reduced);
			Assert.IsType<SqlConstantExpression>(reduced);

			var result = ((SqlConstantExpression)reduced).Value;
			var expectedResult = SqlObject.New(SqlValueUtil.FromObject(expected));

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(SqlExpressionType.Equal, 34, 34, "34 = 34")]
		[InlineData(SqlExpressionType.NotEqual, 190, 21, "190 <> 21")]
		[InlineData(SqlExpressionType.GreaterThan, 12.02, 23.98, "12.02 > 23.98")]
		[InlineData(SqlExpressionType.GreaterThanOrEqual, 110, 20, "110 >= 20")]
		[InlineData(SqlExpressionType.LessThan, 67, 98, "67 < 98")]
		[InlineData(SqlExpressionType.LessThanOrEqual, "abc1234", "abc12345", "'abc1234' <= 'abc12345'")]
		[InlineData(SqlExpressionType.Add, 45, 45, "45 + 45")]
		[InlineData(SqlExpressionType.Subtract, 102, 30, "102 - 30")]
		[InlineData(SqlExpressionType.Multiply, 22, 2, "22 * 2")]
		[InlineData(SqlExpressionType.Divide, 100, 2, "100 / 2")]
		[InlineData(SqlExpressionType.Is, true, true, "TRUE IS TRUE")]
		[InlineData(SqlExpressionType.IsNot, 22.09, false, "22.09 IS NOT FALSE")]
		[InlineData(SqlExpressionType.Or, true, false, "TRUE OR FALSE")]
		[InlineData(SqlExpressionType.XOr, 113, 56, "113 XOR 56")]
		[InlineData(SqlExpressionType.And, true, false, "TRUE AND FALSE")]
		public static void GetBinaryString(SqlExpressionType expressionType, object value1, object value2, string expected) {
			var obj1 = SqlObject.New(SqlValueUtil.FromObject(value1));
			var obj2 = SqlObject.New(SqlValueUtil.FromObject(value2));

			var exp = SqlExpression.Binary(expressionType, SqlExpression.Constant(obj1), SqlExpression.Constant(obj2));

			var s = exp.ToString();
			Assert.Equal(expected, s);
		}


		[Theory]
		[InlineData(SqlExpressionType.Add, 445, 9032.11)]
		public static void GetBinarySqlType(SqlExpressionType expressionType, object value1, object value2) {
			var obj1 = SqlObject.New(SqlValueUtil.FromObject(value1));
			var obj2 = SqlObject.New(SqlValueUtil.FromObject(value2));

			var exp = SqlExpression.Binary(expressionType, SqlExpression.Constant(obj1), SqlExpression.Constant(obj2));

			var sqltType = exp.Type;
			var wider = obj1.Type.Wider(obj2.Type);

			Assert.Equal(wider, sqltType);
		}

		//TODO:
		//[Theory]
		//[InlineData("a + 56", SqlExpressionType.Add)]
		//[InlineData("783.22 * 22", SqlExpressionType.Multiply)]
		//[InlineData("12 / 3", SqlExpressionType.Divide)]
		//[InlineData("67 - 33", SqlExpressionType.Subtract)]
		//[InlineData("11 % 3", SqlExpressionType.Modulo)]
		//[InlineData("a  IS NOT NULL", SqlExpressionType.IsNot)]
		//[InlineData("a IS UNKNOWN", SqlExpressionType.Is)]
		////TODO: [InlineData("a IS OF TYPE VARCHAR(3)", SqlExpressionType.Is)]
		//[InlineData("c > 22", SqlExpressionType.GreaterThan)]
		//[InlineData("178.999 >= 902", SqlExpressionType.GreaterThanOrEqual)]
		//[InlineData("a < -1", SqlExpressionType.LessThan)]
		//[InlineData("189 <= 189", SqlExpressionType.LessThanOrEqual)]
		//[InlineData("a = b", SqlExpressionType.Equal)]
		//[InlineData("a <> c", SqlExpressionType.NotEqual)]
		//[InlineData("TRUE OR FALSE", SqlExpressionType.Or)]
		//[InlineData("a AND b", SqlExpressionType.And)]
		////TODO: [InlineData("674 XOR 90", SqlExpressionType.XOr)]
		//[InlineData("a BETWEEN 56 AND 98", SqlExpressionType.And)]
		//public static void ParseString(string s, SqlExpressionType expressionType) {
		//	var exp = SqlExpression.Parse(s);
			
		//	Assert.NotNull(exp);
		//	Assert.Equal(expressionType, exp.ExpressionType);
		//	Assert.IsType<SqlBinaryExpression>(exp);
		//}
	}
}