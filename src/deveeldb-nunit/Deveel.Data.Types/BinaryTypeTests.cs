using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Types {
	[TestFixture]
	[Category("Data Types")]
	public static class BinaryTypeTests {
		[TestCase("VARBINARY", SqlTypeCode.VarBinary, BinaryType.DefaultMaxSize)]
		[TestCase("BLOB", SqlTypeCode.Blob, BinaryType.DefaultMaxSize)]
		[TestCase("LONG BINARY VARYING", SqlTypeCode.LongVarBinary, BinaryType.DefaultMaxSize)]
		[TestCase("BINARY", SqlTypeCode.Binary, BinaryType.DefaultMaxSize)]
		[TestCase("VARBINARY(2048)", SqlTypeCode.VarBinary, 2048)]
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
