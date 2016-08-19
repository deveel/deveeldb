using System;

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
	}
}
