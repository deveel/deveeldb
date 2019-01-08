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

using Deveel.Data.Serialization;

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

		[Theory]
		[InlineData(SqlTypeCode.VarBinary, -1)]
		[InlineData(SqlTypeCode.Binary, 4556)]
		public static void Serialize(SqlTypeCode typeCode, int size)
		{
			var type = new SqlBinaryType(typeCode, size);
			var result = BinarySerializeUtil.Serialize(type);

			Assert.Equal(type, result);
			Assert.Equal(size, type.MaxSize);
		}

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