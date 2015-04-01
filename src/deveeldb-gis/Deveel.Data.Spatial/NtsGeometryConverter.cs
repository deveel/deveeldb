using System;

using NetTopologySuite.Geometries;

namespace Deveel.Data.Spatial {
	static class NtsGeometryConverter {
		public static IGeometry Convert(IGeometryFactory factory, GeoAPI.Geometries.IGeometry geometry) {
			throw new NotImplementedException();
		}

		public static IGeometry Convert(IGeometryFactory factory, Geometry geometry) {
			throw new NotImplementedException();
		}

		public static GeoAPI.Geometries.IGeometry Convert(IGeometryFactory factory, IGeometry geometry) {
			throw new NotImplementedException();
		}
	}
}
