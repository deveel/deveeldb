using System;

using Deveel.Data.Services;
using Deveel.Data.Spatial;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public sealed class DataTypeParseTests : ContextBasedTest {
		protected override void RegisterServices(ServiceContainer container) {
			container.UseSpatial();
		}

		[Test]
		public void SimpleGeometryWithNoContext() {
			const string typeString = "GEOMETRY";

			Assert.Throws<NotSupportedException>(() => SqlType.Parse(typeString));
		}

		[Test]
		public void SimpleGeometry() {
			const string typeString = "GEOMETRY";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(Query.Context, typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<SpatialType>(type);
			Assert.AreEqual(-1, ((SpatialType)type).Srid);
		}

		[Test]
		public void GeometryWithSrid() {
			const string typeString = "GEOMETRY(SRID = '2029')";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(Query.Context, typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<SpatialType>(type);
			Assert.AreEqual(2029, ((SpatialType)type).Srid);
		}
	}
}
