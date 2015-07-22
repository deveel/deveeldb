using System;
using System.IO;

using Deveel.Data.DbSystem;
using Deveel.Data.Objects;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

using GeoAPI;
using GeoAPI.Geometries;

using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Deveel.Data.Spatial {
	public static class SpatialSystemFunctions {
		static SpatialSystemFunctions() {
			Resolver = new SpatialFunctionProvider();
		}

		public static IRoutineResolver Resolver { get; private set; }

		// TODO: Make these ones configurable and in context...
		private static readonly IGeometryServices GeometryServices = new NtsGeometryServices();
		private static readonly IGeometryFactory GeometryFactory = new GeometryFactory();

		public static SqlGeometry FromWkb(SqlBinary source) {
			var reader = new WKBReader(GeometryServices);

			IGeometry geometry;
			using (var stream = new MemoryStream(source.ToByteArray(), false)) {
				geometry = reader.Read(stream);
			}

			return new SqlGeometry(geometry);
		}

		public static DataObject FromWkb(DataObject source) {
			var input = (SqlBinary) source.Value;
			var result = FromWkb(input);
			return new DataObject(SpatialType.Geometry(), result);
		}

		public static DataObject FromWkt(DataObject source) {
			var input = (SqlString) source.Value;
			var result = FromWkt(input);
			return new DataObject(SpatialType.Geometry(), result);
		}

		public static SqlGeometry FromWkt(SqlString source) {
			var reader = new WKTReader(GeometryFactory);

			IGeometry geometry;
			using (var textReader = source.GetInput()) {
				geometry = reader.Read(textReader);
			}

			return new SqlGeometry(geometry);
		}
	}
}
