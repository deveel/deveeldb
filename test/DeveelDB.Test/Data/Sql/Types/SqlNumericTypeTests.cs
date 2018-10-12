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
	public static class SqlNumericTypeTests {
		[Theory]
		[InlineData(SqlTypeCode.Integer, 10, 0)]
		[InlineData(SqlTypeCode.Numeric, 20, 15)]
		[InlineData(SqlTypeCode.BigInt, 19, 0)]
		[InlineData(SqlTypeCode.Numeric, 21, 10)]
		[InlineData(SqlTypeCode.VarNumeric, -1, -1)]
		public static void CreateNumericType(SqlTypeCode typeCode, int precision, int scale) {
			var type = new SqlNumericType(typeCode, precision, scale);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.Equal(precision, type.Precision);
			Assert.Equal(scale, type.Scale);
			Assert.True(type.IsIndexable);
			Assert.True(type.IsPrimitive);
			Assert.False(type.IsLargeObject);
			Assert.False(type.IsReference);
		}

		[Theory]
		[InlineData(4553.0944, 4553.0944, true)]
		[InlineData(322, 321, false)]
		public static void Equal(double value1, double value2, bool expected) {
			BinaryOp(type => type.Equal, value1, value2, expected);
		}

		[Theory]
		[InlineData(10020, 21002, false)]
		[InlineData(32.1002, 31.223334, true)]
		[InlineData(10223933, 1233.903, true)]
		public static void Greater(double value1, double value2, bool expected) {
			BinaryOp(type => type.Greater, value1, value2, expected);
		}

		[Theory]
		[InlineData(3212, 1022333.322, true)]
		[InlineData(2123e89, 102223e21, false)]
		[InlineData(122, 100, false)]
		public static void Smaller(double value1, double value2, bool expected) {
			BinaryOp(type => type.Less, value1, value2, expected);
		}

		[Theory]
		[InlineData(2344, 23456, false)]
		[InlineData(1233, 1233, true)]
		[InlineData(4321.34e32, 2112.21e2, true)]
		public static void GreateOrEqual(double value1, double value2, bool expected) {
			BinaryOp(type => type.GreaterOrEqual, value1, value2, expected);
		}


		[Theory]
		[InlineData(2133, 100, false)]
		[InlineData(210, 4355e45, true)]
		public static void SmallerOrEqual(double value1, double value2, bool expected) {
			BinaryOp(type => type.LessOrEqual, value1, value2, expected);
		}

		[Theory]
		[InlineData(566, -567)]
		[InlineData(789929.245, -789930)]
		[InlineData((byte)1, -2)]
		public static void Not(object value, object expected) {
			OperatorsUtil.Unary(type => type.Not, value, expected);
		}

		[Theory]
		[InlineData(112, 112)]
		[InlineData(-98.09f, -98.09f)]
		public static void UnaryPlus(object value, object expected) {
			OperatorsUtil.Unary(type => type.UnaryPlus, value, expected);
		}

		[Theory]
		[InlineData(-2536.9039, 2536.9039)]
		[InlineData(788, -788)]
		public static void Negate(object value, object expected) {
			OperatorsUtil.Unary(type => type.Negate, value, expected);
		}

		[Theory]
		[InlineData(4355, SqlTypeCode.Double, -1, -1, (double) 4355)]
		[InlineData(673.04492, SqlTypeCode.String, 200, -1, "673.04492")]
		[InlineData(6709.89f, SqlTypeCode.Char, 7, -1, "6709.89")]
		[InlineData((byte)23, SqlTypeCode.Double, -1, -1, (double)23)]
		[InlineData(32167, SqlTypeCode.Float, -1, -1, (float)32167)]
		[InlineData((double)56878.99876, SqlTypeCode.Float, -1, -1, (float) 56878.99876)]
		private static void Cast(object value, SqlTypeCode destType, int p, int s, object expected) {
			OperatorsUtil.Cast(value, destType, p, s, expected);
		}

		private static void BinaryOp(Func<SqlType, Func<ISqlValue, ISqlValue, SqlBoolean>> selector, object value1, object value2, bool expected) {
			OperatorsUtil.Binary(PrimitiveTypes.Double(), selector, value1, value2, expected);
		}

		[Theory]
		[InlineData(SqlTypeCode.TinyInt, -1, -1, "TINYINT")]
		[InlineData(SqlTypeCode.SmallInt, -1, -1, "SMALLINT")]
		[InlineData(SqlTypeCode.Integer, -1, -1, "INTEGER")]
		[InlineData(SqlTypeCode.BigInt, -1, -1, "BIGINT")]
		[InlineData(SqlTypeCode.Float, -1, -1, "FLOAT")]
		[InlineData(SqlTypeCode.Real, -1, -1, "FLOAT")]
		[InlineData(SqlTypeCode.Decimal, -1, -1, "DECIMAL")]
		[InlineData(SqlTypeCode.Numeric, 23, 5, "NUMERIC(23,5)")]
		[InlineData(SqlTypeCode.VarNumeric, -1, -1, "VARNUMERIC")]
		public static void GetString(SqlTypeCode typeCode, int p, int s, string expected) {
			var type = PrimitiveTypes.Type(typeCode, new {precision = p, scale = s});
			Assert.NotNull(type);
			Assert.IsType<SqlNumericType>(type);

			var sqlString = type.ToString();

			Assert.Equal(expected, sqlString);
		}

		//TODO:
	    //[Theory]
	    //[InlineData("INT", SqlTypeCode.Integer, 10, 0)]
	    //[InlineData("INTEGER", SqlTypeCode.Integer, 10, 0)]
	    //[InlineData("BIGINT", SqlTypeCode.BigInt, 19, 0)]
	    //[InlineData("SMALLINT", SqlTypeCode.SmallInt, 5, 0)]
	    //[InlineData("TINYINT", SqlTypeCode.TinyInt, 3, 0)]
	    //[InlineData("DOUBLE", SqlTypeCode.Double, 16, -1)]
	    //[InlineData("FLOAT", SqlTypeCode.Float, 8, -1)]
	    //[InlineData("REAL", SqlTypeCode.Float, 8, -1)]
	    //[InlineData("DECIMAL", SqlTypeCode.Decimal, 24, -1)]
     //   [InlineData("NUMERIC(22, 13)", SqlTypeCode.Numeric, 22, 13)]
	    //public static void ParseString(string sql, SqlTypeCode typeCode, int precision, int scale) {
	    //    var type = SqlType.Parse(sql);

	    //    Assert.NotNull(type);
	    //    Assert.Equal(typeCode, type.TypeCode);
	    //    Assert.IsType<SqlNumericType>(type);

	    //    var numericType = (SqlNumericType) type;

	    //    Assert.Equal(precision, numericType.Precision);
	    //    Assert.Equal(scale, numericType.Scale);
	    //}


	    [Theory]
		[InlineData(544667.002f, SqlTypeCode.VarBinary, 200)]
		[InlineData(6734, SqlTypeCode.VarBinary, 56)]
		[InlineData(900192299.9220, SqlTypeCode.VarBinary, 450)]
		public static void CastToBinary(object value, SqlTypeCode typeCode, int size) {
			var type = SqlTypeUtil.FromValue(value);
			var destType = PrimitiveTypes.Binary(typeCode, size);

			Assert.NotNull(type);
			Assert.IsType<SqlNumericType>(type);

			var number = (SqlNumber) SqlValueUtil.FromObject(value);

			Assert.True(type.CanCastTo(number, destType));
			var result = type.Cast(number, destType);

			Assert.IsAssignableFrom<ISqlBinary>(result);

			var binary = (ISqlBinary) result;
			
			var memStream = new MemoryStream();
			binary.GetInput().CopyTo(memStream);

			var bytes = memStream.ToArray();
			Assert.NotEmpty(bytes);

			var back = new SqlNumber(bytes);

			Assert.Equal(number, back);
		}

		[Theory]
		[InlineData(SqlTypeCode.BigInt, null, null, 50L, true)]
		[InlineData(SqlTypeCode.Integer, null, null, 180000000000L, false)]
		[InlineData(SqlTypeCode.Integer, null, null, 22, true)]
		public static void IsInstanceOf(SqlTypeCode typeCode, int? precision, byte? scale, object value, bool expected) {
			var type = new SqlNumericType(typeCode, precision ?? -1, scale ?? -1);
			var number = SqlValueUtil.FromObject(value);

			Assert.Equal(expected, type.IsInstanceOf(number));
		}
	}
}