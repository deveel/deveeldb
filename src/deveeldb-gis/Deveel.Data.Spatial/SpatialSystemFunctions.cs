using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Objects;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

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

		public static DataObject ToWkb(DataObject geometry) {
			if (geometry.IsNull)
				return DataObject.Null(PrimitiveTypes.String());

			var g = (SqlGeometry)geometry.Value;
			return DataObject.Binary(ToWkb(g));
		}

		public static SqlBinary ToWkb(SqlGeometry geometry) {
			if (geometry == null || geometry.IsNull)
				return SqlBinary.Null;

			return geometry.ToWellKnownBytes();
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

		public static SqlString ToWkt(SqlGeometry geometry) {
			if (geometry == null || geometry.IsNull)
				return SqlString.Null;

			return geometry.ToWellKnownText();
		}

		public static DataObject ToWkt(DataObject geometry) {
			if (geometry.IsNull)
				return DataObject.Null(PrimitiveTypes.String());

			var g = (SqlGeometry) geometry.Value;
			return DataObject.String(ToWkt(g));
		}

		public static DataObject Envelope(DataObject geometry) {
			var input = (SqlGeometry) geometry.Value;
			var envelope = Envelope(input);
			return new DataObject(SpatialType.Geometry(), envelope);
		}

		public static SqlGeometry Envelope(SqlGeometry geometry) {
			if (geometry.IsNull)
				return SqlGeometry.Null;

			return geometry.Envelope;
		}

		public static DataObject Distance(DataObject geometry, DataObject other) {
			var input = (SqlGeometry)geometry.Value;
			var otherGeometry = (SqlGeometry) other.Value;
			var result = Distance(input, otherGeometry);
			return DataObject.Number(result);
		}

		public static SqlNumber Distance(SqlGeometry geometry, SqlGeometry other) {
			if (geometry == null || geometry.IsNull)
				return SqlNumber.Null;

			return geometry.Distance(other);
		}

		public static SqlBoolean Contains(SqlGeometry geometry, SqlGeometry other) {
			if (geometry == null || geometry.IsNull)
				return SqlBoolean.Null;

			return geometry.Contains(other);
		}

		public static DataObject Contains(DataObject geometry, DataObject other) {
			var g1 = (SqlGeometry) geometry.Value;
			var g2 = (SqlGeometry) other.Value;
			var result = Contains(g1, g2);
			return DataObject.Boolean(result);
		}
	}
}
