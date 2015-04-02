using System;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public sealed class DataTypeParseTests {
		[Test]
		public static void SimpleGeometry() {
			const string typeString = "GEOMETRY";

			DataType type = null;
			Assert.DoesNotThrow(() => type = DataType.Parse(typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<GeometryType>(type);
			Assert.AreEqual(-1, ((GeometryType)type).Srid);
		}

		[Test]
		public static void GeometryWithSrid() {
			const string typeString = "GEOMETRY(2029)";

			DataType type = null;
			Assert.DoesNotThrow(() => type = DataType.Parse(typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<GeometryType>(type);
			Assert.AreEqual(2029, ((GeometryType)type).Srid);
		}
	}
}
