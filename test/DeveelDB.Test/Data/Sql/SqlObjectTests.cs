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
	public static class SqlObjectTests {
		[Fact]
		public static void NullCheck() {
			var obj = SqlObject.Null;

			Assert.NotNull(obj);
			Assert.IsType<SqlObject>(obj);
			Assert.True(obj.IsNull);
			Assert.False(obj.IsUnknown);
			Assert.False(obj.IsTrue);
			Assert.False(obj.IsFalse);
		}

		[Theory]
		[InlineData(SqlTypeCode.Double, 16, -1, 36755.0912)]
		[InlineData(SqlTypeCode.Integer, -1, -1, 54667)]
		public static void GetNumericObject(SqlTypeCode code, int precision, int scale, double value) {
			var type = new SqlNumericType(code, precision, scale);
			var number = (SqlNumber) value;

			var obj = new SqlObject(type, number);

			Assert.Equal(type, obj.Type);
			Assert.Equal(number, obj.Value);
			Assert.False(obj.IsNull);
		}

		[Theory]
		[InlineData(SqlTypeCode.Char, 12, "hello!", "hello!      ")]
		[InlineData(SqlTypeCode.VarChar, 255, "hello!", "hello!")]
		public static void GetStringObject(SqlTypeCode code, int maxSize, string value, string expected) {
			var type = new SqlCharacterType(code, maxSize, null);
			var s = new SqlString(value);

			var obj = new SqlObject(type, s);
			Assert.Equal(type, obj.Type);
			Assert.NotNull(obj.Value);
			Assert.Equal(expected, obj.Value.ToString());
			Assert.False(obj.IsNull);
		}

		[Theory]
		[InlineData("test1", "test1", true)]
		[InlineData("test2", "test1", false)]
		public static void StringEqualToString(string s1, string s2, bool expected) {
			var obj1 = SqlObject.String((SqlString) s1);
			var obj2 = SqlObject.String((SqlString) s2);

			var result = obj1.Equals(obj2);

			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(SqlTypeCode.BigInt)]
		public static void NullObject_FromSqlNull(SqlTypeCode code) {
			var type = PrimitiveTypes.Type(code);
			var obj = new SqlObject(type, SqlNull.Value);

			Assert.Equal(code, obj.Type.TypeCode);
			Assert.Equal(type, obj.Type);
			Assert.IsType<SqlNull>(obj.Value);
		}

		[Theory]
		[InlineData(SqlTypeCode.String, SqlTypeCode.BigInt)]
		[InlineData(SqlTypeCode.Boolean, SqlTypeCode.VarChar)]
		public static void EqualNullToNull(SqlTypeCode typeCode1, SqlTypeCode typeCode2) {
			var type1 = PrimitiveTypes.Type(typeCode1);
			var type2 = PrimitiveTypes.Type(typeCode2);

			var obj1 = new SqlObject(type1, SqlNull.Value);
			var obj2 = new SqlObject(type2, SqlNull.Value);

			var result = obj1.Equal(obj2);
			var expectedResult = SqlObject.Boolean(null);

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(SqlTypeCode.Date, SqlTypeCode.Integer)]
		[InlineData(SqlTypeCode.Boolean, SqlTypeCode.Boolean)]
		public static void NotEqualNullToNull(SqlTypeCode typeCode1, SqlTypeCode typeCode2) {
			var type1 = PrimitiveTypes.Type(typeCode1);
			var type2 = PrimitiveTypes.Type(typeCode2);

			var obj1 = new SqlObject(type1, SqlNull.Value);
			var obj2 = new SqlObject(type2, SqlNull.Value);

			var result = obj1.NotEqual(obj2);
			var expectedResult = SqlObject.Boolean(null);

			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(34454655, SqlTypeCode.Integer)]
		[InlineData(-45337782, SqlTypeCode.Integer)]
		[InlineData((short)3445, SqlTypeCode.SmallInt)]
		[InlineData((short)-4533, SqlTypeCode.SmallInt)]
		[InlineData((long)34454655344, SqlTypeCode.BigInt)]
		[InlineData((long)-453377822144, SqlTypeCode.BigInt)]
		[InlineData(223.019f, SqlTypeCode.Float)]
		[InlineData(-0.2f, SqlTypeCode.Float)]
		[InlineData(45533.94044, SqlTypeCode.Double)]
		[InlineData("the quick brown fox", SqlTypeCode.String)]
		public static void NewFromObject(object value, SqlTypeCode expectedType) {
			var obj = FromObject(value);

			Assert.Equal(expectedType, obj.Type.TypeCode);
			Assert.NotNull(obj.Value);
			Assert.False(obj.IsNull);
			Assert.True(obj.Type.IsInstanceOf(obj.Value));
		}

		[Theory]
		[InlineData(3004.330, "3004.33")]
		[InlineData(true, "TRUE")]
		[InlineData(false, "FALSE")]
		[InlineData("the quick brown fox", "the quick brown fox")]
		[InlineData(SqlTypeCode.Unknown, "UNKNOWN")]
		public static void GetAsString(object value, string expected) {
			var obj = FromObject(value);
			Assert.Equal(expected, obj.ToString());
		}

		[Theory]
		[InlineData(SqlTypeCode.Integer, 300, 210, 1)]
		[InlineData(SqlTypeCode.VarChar, "the quick", "the quick brown fox", -32)]
		[InlineData(SqlTypeCode.Boolean, true, true, 0)]
		[InlineData(SqlTypeCode.Boolean, false, true, -1)]
		[InlineData(SqlTypeCode.VarChar, null, "the quick brown fox", -1)]
		[InlineData(SqlTypeCode.Boolean, true, null, 1)]
		public static void Compare(SqlTypeCode typeCode, object value1, object value2, int expected) {
			var type = PrimitiveTypes.Type(typeCode);
			var obj1 = new SqlObject(type, SqlValueUtil.FromObject(value1));
			var obj2 = new SqlObject(type, SqlValueUtil.FromObject(value2));

			var result = obj1.CompareTo(obj2);
			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData(2334.93f, 10.03f, false)]
		[InlineData(93044.33494003, 93044.33494003, true)]
		[InlineData("the quick brown fox", "the quick brown fox ", false)]
		[InlineData("the quick brown fox", "the quick brown fox", true)]
		[InlineData(56, 45, false)]
		[InlineData(546, null, SqlTypeCode.Unknown)]
		public static void Operator_Equal(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Equal(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(false, false, false)]
		[InlineData(true, false, true)]
		[InlineData("The quick brown Fox", "the quick brown fox", true)]
		[InlineData(9042.55f, 223.092f, true)]
		[InlineData(null, 1902, SqlTypeCode.Unknown)]
		public static void Operator_NotEqual(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.NotEqual(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(456, 223, true)]
		[InlineData("the quick brown", "the quick brown fox", true)]
		public static void Operator_GreaterThan(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.GreaterThan(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(647483.9930, 192e43, true)]
		public static void Operator_LessThan(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.LessThan(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(659, 659, true)]
		[InlineData(43222, 10e34, false)]
		[InlineData(1922.333, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		[InlineData(null, 903.400, SqlTypeCode.Unknown)]
		public static void Operator_GreaterOrEqualThan(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.GreaterThanOrEqual(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(74644, 100, false)]
		[InlineData("the quick brown fox", "the quick brown fox", true)]
		[InlineData(7849, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		public static void Operator_LessOrEqualThan(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.LessOrEqualThan(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(SqlTypeCode.Unknown, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		[InlineData(true, SqlTypeCode.Unknown, true)]
		[InlineData(false, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		public static void Operator_Or(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Or(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(SqlTypeCode.Unknown, false, false)]
		[InlineData(SqlTypeCode.Unknown, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		[InlineData(true, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		public static void Operator_And(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.And(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(2334.0923e21, 0912.09e2, 2.3340923e24)]
		[InlineData(99304, null, null)]
		public static void Operator_Add(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Add(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(56.0031, 0911.222, -855.2189)]
		[InlineData(839440.331, null, null)]
		[InlineData(3923, SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		public static void Operator_Subtract(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Subtract(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(84, 2, 42)]
		public static void Operator_Divide(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Divide(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(5893.657, 3445, 2448.657)]
		public static void Operator_Modulo(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Modulo(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(588.36, 65.2, 38361.072)]
		public static void Operator_Multiply(object value1, object value2, object expected) {
			BinaryOp((x, y) => x.Multiply(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(true, true, true)]
		[InlineData(true, false, false)]
		[InlineData(false, false, true)]
		[InlineData(SqlTypeCode.Unknown, SqlTypeCode.Unknown, true)]
		[InlineData(SqlTypeCode.Unknown, true, false)]
		[InlineData(SqlTypeCode.Unknown, false, false)]
		[InlineData(3445.22, SqlTypeCode.Unknown, false)]
		public static void Operator_Is(object value1, object value2, bool expected) {
			BinaryOp((x, y) => x.Is(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(true, true, false)]
		[InlineData(false,true, true)]
		[InlineData(false, false, false)]
		[InlineData(SqlTypeCode.Unknown, SqlTypeCode.Unknown, false)]
		[InlineData(SqlTypeCode.Unknown, true, true)]
		[InlineData(SqlTypeCode.Unknown, false, true)]
		[InlineData(65884.223, SqlTypeCode.Unknown, true)]
		[InlineData("the quick brown fox", true, true)]
		public static void Operator_IsNot(object value1, object value2, bool expected) {
			BinaryOp((x, y) => x.IsNot(y), value1, value2, expected);
		}

		[Theory]
		[InlineData(true, false)]
		[InlineData(false, true)]
		[InlineData(SqlTypeCode.Unknown, SqlTypeCode.Unknown)]
		[InlineData(-5603.0032, 5603.0032)]
		public static void Operator_Not(object value, object expected) {
			UnaryOp(x => x.Not(), value, expected);
		}

		[Theory]
		[InlineData(903, 903)]
		[InlineData(-56.9930, -56.9930)]
		public static void Operator_Plus(object value, object expected) {
			UnaryOp(x => x.Plus(), value, expected);
		}

		private static void UnaryOp(Func<SqlObject, SqlObject> op, object value, object expected) {
			var obj = FromObject(value);
			var result = op(obj);

			var expectedObj = FromObject(expected);
			if (expectedObj.CanCastTo(result.Type))
				expectedObj = expectedObj.CastTo(result.Type);

			Assert.Equal(expectedObj, result);
		}

		private static void BinaryOp(Func<SqlObject, SqlObject, SqlObject> op, object value1, object value2, object expected) {
			var obj1 = FromObject(value1);
			var obj2 = FromObject(value2);

			var result = op(obj1, obj2);

			var expectedObj = FromObject(expected);

			if (expectedObj.CanCastTo(result.Type))
				expectedObj = expectedObj.CastTo(result.Type);

			Assert.Equal(expectedObj, result);
		}

		private static SqlObject FromObject(object value) {
			if (value == null)
				return SqlObject.Null;

			if (value is SqlTypeCode &&
				(SqlTypeCode)value == SqlTypeCode.Unknown)
				return SqlObject.Unknown;

			var sqlValue = SqlValueUtil.FromObject(value);
			var sqlType = SqlTypeUtil.FromValue(value);
			return new SqlObject(sqlType, sqlValue);
		}
	}
}