using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Objects;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Spatial {
	public static class SpatialSystemFunctions {
		static SpatialSystemFunctions() {
			Resolver = new SpatialFunctionProvider();
		}

		public static IRoutineResolver Resolver { get; private set; }

		public static SqlGeometry FromWkb(SqlBinary source) {
			if (source.IsNull)
				return SqlGeometry.Null;

			SqlGeometry geometry;
			if (!SqlGeometry.TryParse(source.ToByteArray(), out geometry))
				return SqlGeometry.Null;

			return geometry;
		}

		public static DataObject FromWkb(DataObject source) {
			var input = (SqlBinary) source.Value;
			var result = FromWkb(input);
			return new DataObject(SpatialType.Geometry(), result);
		}

		public static DataObject FromWkt(IQueryContext context, DataObject source) {
			var input = (SqlString) source.Value;
			var result = FromWkt(input);
			return new DataObject(SpatialType.Geometry(), result);
		}

		public static SqlGeometry FromWkt(SqlString source) {
			SqlGeometry geometry;
			if (!SqlGeometry.TryParse(source.ToString(), out geometry))
				return SqlGeometry.Null;

			return geometry;
		}

		public static DataObject Envelope(DataObject geometry) {
			var input = (SqlGeometry) geometry.Value;
			var envelope = Envelope(input);
			return new DataObject(SpatialType.Geometry(), envelope);
		}

		private static SqlGeometry Envelope(SqlGeometry geometry) {
			if (geometry.IsNull)
				return SqlGeometry.Null;

			return geometry.Envelope;
		}
	}
}
