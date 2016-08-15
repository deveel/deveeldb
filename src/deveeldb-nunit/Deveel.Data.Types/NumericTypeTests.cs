using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public static class NumericTypeTests {
		[TestCase(SqlTypeCode.TinyInt, typeof(byte))]
		[TestCase(SqlTypeCode.SmallInt, typeof(short))]
		[TestCase(SqlTypeCode.Integer, typeof(int))]
		[TestCase(SqlTypeCode.BigInt, typeof(long))]
		[TestCase(SqlTypeCode.Float, typeof(float))]
		[TestCase(SqlTypeCode.Double, typeof(double))]
		[TestCase(SqlTypeCode.Real, typeof(float))]
		[TestCase(SqlTypeCode.Numeric, typeof(SqlNumber))]
		public static void GetRuntime(SqlTypeCode code, Type expectedType) {
			var type = PrimitiveTypes.Numeric(code);

			var runtimeType = type.GetRuntimeType();
			Assert.AreEqual(expectedType, runtimeType);
		}

		[TestCase(SqlTypeCode.TinyInt, 2, 0, (byte)24)]
		[TestCase(SqlTypeCode.Integer, 3, 0, (byte)120)]
		[TestCase(SqlTypeCode.BigInt, 6, 0, 560012)]
		public static void CreateFrom(SqlTypeCode code, int precision, int scale, object value) {
			var type = PrimitiveTypes.Numeric(code, precision, scale);

			var result = type.CreateFrom(value);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<SqlNumber>(result);

			var number = (SqlNumber) result;
			Assert.AreEqual(scale, number.Scale);
			Assert.AreEqual(precision, number.Precision);
		}

		[Test]
		public static void SimpleTypesEqual() {
			var type1 = PrimitiveTypes.Integer(255);
			var type2 = PrimitiveTypes.Integer(255);

			Assert.AreEqual(type1, type2);
		}
	}
}
