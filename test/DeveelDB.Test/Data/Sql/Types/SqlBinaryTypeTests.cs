using System;
using System.IO;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class SqlBinaryTypeTests {
		[Fact]
		public static void CastToNumber() {
			var type = new SqlBinaryType(SqlTypeCode.Binary);

			var value = new SqlBinary(new byte[]{44, 95, 122, 0});
			Assert.True(type.CanCastTo(value, PrimitiveTypes.Numeric(22, 4)));

			var result = type.Cast(value, PrimitiveTypes.Numeric(22, 4));

			Assert.NotNull(result);
			Assert.IsType<SqlNumber>(result);

			var number = (SqlNumber) result;
			Assert.NotNull(number);
			Assert.False(number.CanBeInt32);
			Assert.Equal(74445.4656, (double) number);
		}

		[Theory]
		[InlineData((byte)1, true)]
		[InlineData((byte)0, false)]
		public static void CastToBoolean(byte singleByte, bool expected) {
			var type = new SqlBinaryType(SqlTypeCode.Binary);

			var value = new SqlBinary(new[]{singleByte});
			Assert.True(type.CanCastTo(value, PrimitiveTypes.Bit()));

			var result = type.Cast(value, PrimitiveTypes.Bit());

			Assert.NotNull(result);
			Assert.IsType<SqlBoolean>(result);

			Assert.Equal(expected, (bool?)((SqlBoolean)result));
		}

		[Theory]
		[InlineData("the quick brown fox", SqlTypeCode.VarChar, -1, "the quick brown fox")]
		[InlineData("the quick brown fox", SqlTypeCode.Char, 10, "the quick ")]
		public static void CastToString(string s, SqlTypeCode typeCode, int maxSize, string expected) {
			var type = new SqlBinaryType(SqlTypeCode.Binary);
			var input = (SqlString)s;
			var destType = new SqlCharacterType(typeCode, maxSize, null);

			var bytes = input.ToByteArray();
			var binary = new SqlBinary(bytes);

			Assert.True(type.CanCastTo(binary, destType));

			var result = type.Cast(binary, destType);

			Assert.NotNull(result);
			Assert.IsType<SqlString>(result);

			Assert.Equal(expected, (SqlString)result);
		}

		[Theory]
		[InlineData("2010-02-11", SqlTypeCode.Date)]
		public static void CastToDate(string s, SqlTypeCode typeCode) {
			var type = new SqlBinaryType(SqlTypeCode.Binary);
			var date = SqlDateTime.Parse(s);
			var destType = new SqlDateTimeType(typeCode);

			var bytes = date.ToByteArray();
			var binary = new SqlBinary(bytes);

			Assert.True(type.CanCastTo(binary, destType));

			var result = type.Cast(binary, destType);

			Assert.NotNull(result);
			Assert.IsType<SqlDateTime>(result);

			Assert.Equal(date, (SqlDateTime)result);
		}

		[Theory]
		[InlineData(SqlTypeCode.VarBinary, -1, "VARBINARY")]
		[InlineData(SqlTypeCode.Binary, 4556, "BINARY(4556)")]
		public static void GetString(SqlTypeCode typeCode, int size, string expected) {
			var type = new SqlBinaryType(typeCode, size);

			var s = type.ToString();
			Assert.Equal(expected, s);
		}

		// TODO:
	    //[Theory]
	    //[InlineData("VARBINARY", SqlTypeCode.VarBinary, -1)]
	    //[InlineData("VARBINARY(1024)", SqlTypeCode.VarBinary, 1024)]
	    //[InlineData("BINARY(2048)", SqlTypeCode.Binary, 2048)]
	    //[InlineData("BLOB", SqlTypeCode.Blob, -1)]
	    //[InlineData("BLOB(45644)", SqlTypeCode.Blob, 45644)]
	    //[InlineData("BINARY(MAX)", SqlTypeCode.Binary, SqlBinaryType.DefaultMaxSize)]
	    //[InlineData("LONG BINARY VARYING", SqlTypeCode.LongVarBinary, -1)]
	    //public static void ParseString(string sql, SqlTypeCode typeCode, int size) {
	    //    var type = SqlType.Parse(sql);

	    //    Assert.NotNull(type);
	    //    Assert.Equal(typeCode, type.TypeCode);
	    //    Assert.IsType<SqlBinaryType>(type);

	    //    var binType = (SqlBinaryType) type;

	    //    Assert.Equal(size, binType.MaxSize);
	    //}

	 //   [Theory]
		//[InlineData(SqlTypeCode.VarBinary, -1)]
		//[InlineData(SqlTypeCode.Binary, 4556)]
		//public static void Serialize(SqlTypeCode typeCode, int size) {
		//	var type = new SqlBinaryType(typeCode, size);
		//	var result = BinarySerializeUtil.Serialize(type);

		//	Assert.Equal(type, result);
		//}

		[Theory]
		[InlineData(SqlTypeCode.Binary, -1, SqlTypeCode.Binary, -1, true)]
		[InlineData(SqlTypeCode.VarBinary, 455, SqlTypeCode.VarBinary, -1, false)]
		[InlineData(SqlTypeCode.Binary, 1024, SqlTypeCode.VarBinary, 1024, false)]
		public static void BinaryTypesEqual(SqlTypeCode typeCode1, int size1, SqlTypeCode typeCode2, int size2, bool expected) {
			var type1 = new SqlBinaryType(typeCode1, size1);
			var type2 = new SqlBinaryType(typeCode2, size2);

			Assert.Equal(expected, type1.Equals(type2));
		}

		[Fact]
		public static void BinaryTypeNotEqualToOtherType() {
			var type1 = new SqlBinaryType(SqlTypeCode.VarBinary);
			var type2 = new SqlBooleanType(SqlTypeCode.Bit);

			Assert.False(type1.Equals(type2));
		}

		//[Fact]
		//public static void SerializeValue() {
		//	var type = PrimitiveTypes.Binary(500);
		//	var value = new SqlBinary(new byte[] {25, 87, 0, 156, 77});

		//	var stream = new MemoryStream();
		//	type.Serialize(stream, value);

		//	stream.Seek(0, SeekOrigin.Begin);

		//	var value2 = type.Deserialize(stream);

		//	Assert.Equal(value, value2);
		//}
	}
}