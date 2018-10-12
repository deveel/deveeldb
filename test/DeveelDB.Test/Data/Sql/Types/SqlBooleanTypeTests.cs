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
using System.IO;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public class SqlBooleanTypeTests {
		[Fact]
		public void Compare_Booleans() {
			var type = new SqlBooleanType(SqlTypeCode.Boolean);
			Assert.NotNull(type);

			Assert.Equal(1, type.Compare(SqlBoolean.True, SqlBoolean.False));
			Assert.Equal(-1, type.Compare(SqlBoolean.False, SqlBoolean.True));
			Assert.Equal(0, type.Compare(SqlBoolean.True, SqlBoolean.True));
			Assert.Equal(0, type.Compare(SqlBoolean.False, SqlBoolean.False));
		}

		[Fact]
		public static void Add() {
			var a = SqlBoolean.True;
			var b = SqlBoolean.False;
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var result = type.Add(a, b);
			Assert.IsType<SqlNull>(result);
		}

		[Fact]
		public static void Subtract() {
			var a = SqlBoolean.True;
			var b = SqlBoolean.False;
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var result = type.Multiply(a, b);
			Assert.IsType<SqlNull>(result);
		}

		[Fact]
		public static void Multiply() {
			var a = SqlBoolean.True;
			var b = SqlBoolean.False;
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var result = type.Multiply(a, b);
			Assert.IsType<SqlNull>(result);
		}

		[Theory]
		[InlineData(true, false, false)]
		[InlineData(true, true, true)]
		public static void Equal(bool a, bool b, bool expected) {
			var x = (SqlBoolean)a;
			var y = (SqlBoolean)b;
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var result = type.Equal(x, y);
			var exp = (SqlBoolean) expected;

			Assert.Equal(exp, result);
		}

		[Theory]
		[InlineData(true, false, true)]
		[InlineData(true, true, false)]
		public static void NotEqual(bool a, bool b, bool expected) {
			var x = (SqlBoolean)a;
			var y = (SqlBoolean)b;
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var result = type.NotEqual(x, y);
			var exp = (SqlBoolean)expected;

			Assert.Equal(exp, result);
		}

		[Theory]
		[InlineData(true, true, true)]
		[InlineData(true, false, false)]
		public static void And(bool a, bool b, bool expected) {
			BinaryOp(type => type.And, a, b, expected);
		}

		[Theory]
		[InlineData(true, true, true)]
		[InlineData(true, false, true)]
		public static void Or(bool a, bool b, bool expected) {
			BinaryOp(type => type.Or, a, b, expected);
		}

		[Theory]
		[InlineData(true, false)]
		[InlineData(false, true)]
		public static void Negate(bool value, bool expected) {
			var b = (SqlBoolean) value;
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var result = type.Negate(b);
			var exp = (SqlBoolean) expected;

			Assert.Equal(exp, result);
		}

		[Theory]
		[InlineData(true, true, false)]
		[InlineData(false, false, false)]
		[InlineData(true,  false, true)]
		[InlineData(false, true, false)]
		public static void Grater(bool value1, bool value2, bool expected) {
			BinaryOp(type => type.Greater, value1, value2, expected);
		}

		private static void BinaryOp(Func<SqlType, Func<ISqlValue, ISqlValue, SqlBoolean>> selector, bool value1, bool value2,
			bool expected) {
			OperatorsUtil.Binary(PrimitiveTypes.Boolean(), selector, value1, value2, expected);
		}

		private static void BinaryOp(Func<SqlType, Func<ISqlValue, ISqlValue, ISqlValue>> selector, bool value1, bool value2,
			bool expected) {
			OperatorsUtil.Binary(PrimitiveTypes.Boolean(), selector, value1, value2, expected);
		}

		[Theory]
		[InlineData(SqlTypeCode.Bit, "BIT")]
		[InlineData(SqlTypeCode.Boolean, "BOOLEAN")]
		public static void BooleanTypeToString(SqlTypeCode typeCode, string expected) {
			var type = new SqlBooleanType(typeCode);

			var s = type.ToString();
			Assert.Equal(expected, s);
		}

		[Fact]
		public static void TrueToString() {
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var value = SqlBoolean.True;
			var s = type.ToSqlString(value);

			Assert.Equal("TRUE", s);
		}

		[Fact]
		public static void FalseToString() {
			var type = new SqlBooleanType(SqlTypeCode.Boolean);

			var value = SqlBoolean.False;
			var s = type.ToSqlString(value);

			Assert.Equal("FALSE", s);
		}

		[Fact]
		public void Compare_BooleanToNumeric_Invalid() {
			var type = PrimitiveTypes.Boolean();
			Assert.NotNull(type);
			Assert.Throws<ArgumentException>(() => type.Compare(SqlBoolean.True, (SqlNumber)22));
		}

		[Theory]
		[InlineData(SqlTypeCode.Bit, true, SqlTypeCode.String, 20, -1, "1")]
		[InlineData(SqlTypeCode.Bit, false, SqlTypeCode.VarChar, 10, -1, "0")]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.VarChar, 20, -1, "TRUE")]
		[InlineData(SqlTypeCode.Boolean, false, SqlTypeCode.String, 10, -1, "FALSE")]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.TinyInt, -1, -1, (byte)1)]
		[InlineData(SqlTypeCode.Boolean, false, SqlTypeCode.TinyInt, -1, -1, (byte)0)]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.SmallInt, -1, -1, (short)1)]
		[InlineData(SqlTypeCode.Boolean, false, SqlTypeCode.SmallInt, -1, -1, (short)0)]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.Integer, -1, -1, 1)]
		[InlineData(SqlTypeCode.Boolean, false, SqlTypeCode.Integer, -1, -1, 0)]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.BigInt, -1, -1, 1L)]
		[InlineData(SqlTypeCode.Boolean, false, SqlTypeCode.BigInt, -1, -1, 0L)]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.Float, -1, -1, 1f)]
		[InlineData(SqlTypeCode.Boolean, false, SqlTypeCode.Float, -1, -1, 0f)]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.Double, -1, -1, (double)1)]
		[InlineData(SqlTypeCode.Boolean, true, SqlTypeCode.Double, -1, -1, (double)1)]
		public static void CastTo(SqlTypeCode srcTypeCode, bool value, SqlTypeCode destTypeCode, int p, int s, object expected) {
			var b = (SqlBoolean) value;
			var srcType = PrimitiveTypes.Boolean(srcTypeCode);
			var destType = PrimitiveTypes.Type(destTypeCode, new {precision = p, scale = s, maxSize = p});

			Assert.True(srcType.CanCastTo(b, destType));

			var result = srcType.Cast(b, destType);
			var expectedValue = SqlValueUtil.FromObject(expected);

			Assert.Equal(expectedValue, result);
		}

		[Theory]
		[InlineData(SqlTypeCode.DateTime)]
		[InlineData(SqlTypeCode.Time)]
		[InlineData(SqlTypeCode.DateTime)]
		[InlineData(SqlTypeCode.DayToSecond)]
		[InlineData(SqlTypeCode.YearToMonth)]
		public static void CastToInvalid(SqlTypeCode destTypeCode) {
			var value = SqlBoolean.True;
			var type = PrimitiveTypes.Boolean();

			var destType = PrimitiveTypes.Type(destTypeCode);

			Assert.False(type.CanCastTo(value, destType));

			var result = type.Cast(value, destType);
			Assert.Equal(SqlNull.Value, result);
		}

		[Theory]
		[InlineData(true, 1)]
		[InlineData(false, 0)]
		public void CastToBinary(bool value, byte expected) {
			var type = PrimitiveTypes.Boolean();
			var boolean = new SqlBoolean(value);

			var casted = type.Cast(boolean, PrimitiveTypes.VarBinary());

			var expectedArray = new[] {expected};

			Assert.IsType<SqlBinary>(casted);
			Assert.Equal(expectedArray, ((SqlBinary) casted).ToByteArray());
		}

		[Theory]
		[InlineData(SqlTypeCode.Bit, "BIT")]
		[InlineData(SqlTypeCode.Boolean, "BOOLEAN")]
		public void GetString(SqlTypeCode typeCode, string expected) {
			var type = new SqlBooleanType(typeCode);

			var s = type.ToString();
			Assert.Equal(expected, s);
		}

		//TODO:
     //   [Theory]
     //   [InlineData("BOOLEAN", SqlTypeCode.Boolean)]
     //   [InlineData("BIT", SqlTypeCode.Bit)]
	    //public static void ParseString(string s, SqlTypeCode typeCode) {
     //       var type = SqlType.Parse(s);

     //       Assert.NotNull(type);
     //       Assert.Equal(typeCode, type.TypeCode);
     //       Assert.IsType<SqlBooleanType>(type);
     //   }

		[Theory]
		[InlineData(SqlTypeCode.Bit, SqlTypeCode.Bit, true)]
		[InlineData(SqlTypeCode.Boolean, SqlTypeCode.Boolean, true)]
		[InlineData(SqlTypeCode.Bit, SqlTypeCode.Boolean, true)]
		public void BooleanTypesEqual(SqlTypeCode typeCode1, SqlTypeCode typeCode2, bool expected) {
			var type1 = new SqlBooleanType(typeCode1);
			var type2 = new SqlBooleanType(typeCode2);

			Assert.Equal(expected, type1.Equals(type2));
		}

		[Fact]
		public void BooleanTypeNotEqualToOtherType() {
			var type1 = new SqlBooleanType(SqlTypeCode.Boolean);
			var type2 = new SqlBinaryType(SqlTypeCode.Binary);

			Assert.False(type1.Equals(type2));
		}
	}
}