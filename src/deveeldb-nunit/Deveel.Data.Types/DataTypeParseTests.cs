using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Spatial;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public sealed class DataTypeParseTests : ContextBasedTest {
		protected override ISystemContext CreateSystemContext() {
			var context = base.CreateSystemContext();
			context.UserSpatial();
			return context;
		}

		[Test]
		public void SimpleGeometryWithNoContext() {
			const string typeString = "GEOMETRY";

			Assert.Throws<NotSupportedException>(() =>  SqlType.Parse(typeString));
		}

		[Test]
		public void SimpleNumeric() {
			const string typeString = "NUMERIC";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<NumericType>(type);
			Assert.AreEqual(-1, ((NumericType)type).Size);
		}


		[Test]
		public void SimpleGeometry() {
			const string typeString = "GEOMETRY";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(QueryContext, typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<SpatialType>(type);
			Assert.AreEqual(-1, ((SpatialType)type).Srid);
		}

		[Test]
		public void GeometryWithSrid() {
			const string typeString = "GEOMETRY(SRID = '2029')";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(QueryContext, typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<SpatialType>(type);
			Assert.AreEqual(2029, ((SpatialType)type).Srid);
		}
	}
}
