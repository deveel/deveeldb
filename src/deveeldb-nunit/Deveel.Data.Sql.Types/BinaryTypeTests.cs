using System;

using Deveel.Data.Sql.Objects;

using NUnit.Framework;

namespace Deveel.Data.Sql.Types {
	[TestFixture]
	[Category("Data Types")]
	public static class BinaryTypeTests {
		[TestCase("VARBINARY", SqlTypeCode.VarBinary, -1)]
		[TestCase("BLOB", SqlTypeCode.Blob, -1)]
		[TestCase("LONG BINARY VARYING", SqlTypeCode.LongVarBinary, -1)]
		[TestCase("BINARY", SqlTypeCode.Binary, -1)]
		[TestCase("VARBINARY(2048)", SqlTypeCode.VarBinary, 2048)]
		[TestCase("VarBinary(Max)", SqlTypeCode.VarBinary, BinaryType.DefaultMaxSize)]
		public static void ParseString(string input, SqlTypeCode expectedTypeCode, int expectedSize) {
			var sqlType = SqlType.Parse(input);

			Assert.IsNotNull(sqlType);
			Assert.IsInstanceOf<BinaryType>(sqlType);
			Assert.AreEqual(expectedTypeCode, sqlType.TypeCode);

			var binType = (BinaryType) sqlType;
			Assert.AreEqual(expectedSize, binType.MaxSize);
		}

		[TestCase(new byte[] {1}, true)]
		[TestCase(new byte[] {0}, false)]
		[TestCase(null, null)]
		public static void ConvertToBoolean(byte[] data, bool? expectedResult) {
			var type = PrimitiveTypes.Binary();
			var binary = new SqlBinary(data);

			var result = type.CastTo(binary, PrimitiveTypes.Boolean());

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<SqlBoolean>(result);

			var boolean = (SqlBoolean) result;

			Assert.AreEqual(expectedResult, (bool?)boolean);
		}

		[TestCase(new byte[] {1,2,3})]
		public static void FailConvertToBoolean(byte[] data) {
			var type = PrimitiveTypes.Binary();
			var binary = new SqlBinary(data);

			Assert.Throws<InvalidCastException>(() => type.CastTo(binary, PrimitiveTypes.Boolean()));
		}
	}
}
