using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Spatial {
	public static class SpatialSystemFunctions {
		public static IRoutineResolver Resolver {
			get { return new SpatialFunctionProvider(); }
		}

		public static SqlGeometry FromWkb(SqlBinary source) {
			if (source.IsNull)
				return SqlGeometry.Null;

			SqlGeometry geometry;
			if (!SqlGeometry.TryParse(source.ToByteArray(), out geometry))
				return SqlGeometry.Null;

			return geometry;
		}

		public static Field FromWkb(Field source) {
			var input = (SqlBinary) source.Value;
			var result = FromWkb(input);
			return new Field(SpatialType.Geometry(), result);
		}

		public static Field ToWkb(Field geometry) {
			if (geometry.IsNull)
				return Field.Null(PrimitiveTypes.String());

			var g = (SqlGeometry)geometry.Value;
			return Field.Binary(ToWkb(g));
		}

		public static SqlBinary ToWkb(SqlGeometry geometry) {
			if (geometry == null || geometry.IsNull)
				return SqlBinary.Null;

			return geometry.ToWellKnownBytes();
		}

		public static Field FromWkt(IRequest context, Field source) {
			var input = (SqlString) source.Value;
			var result = FromWkt(input);
			return new Field(SpatialType.Geometry(), result);
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

		public static Field ToWkt(Field geometry) {
			if (geometry.IsNull)
				return Field.Null(PrimitiveTypes.String());

			var g = (SqlGeometry) geometry.Value;
			return Field.String(ToWkt(g));
		}

		public static Field Envelope(Field geometry) {
			var input = (SqlGeometry) geometry.Value;
			var envelope = Envelope(input);
			return new Field(SpatialType.Geometry(), envelope);
		}

		public static SqlGeometry Envelope(SqlGeometry geometry) {
			if (geometry.IsNull)
				return SqlGeometry.Null;

			return geometry.Envelope;
		}

		public static Field Distance(Field geometry, Field other) {
			var input = (SqlGeometry)geometry.Value;
			var otherGeometry = (SqlGeometry) other.Value;
			var result = Distance(input, otherGeometry);
			return Field.Number(result);
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

		public static Field Contains(Field geometry, Field other) {
			var g1 = (SqlGeometry) geometry.Value;
			var g2 = (SqlGeometry) other.Value;
			var result = Contains(g1, g2);
			return Field.Boolean(result);
		}

		public static Field Area(Field geometry) {
			var input = (SqlGeometry) geometry.Value;
			var result = Area(input);
			return Field.Number(result);
		}

		public static SqlNumber Area(SqlGeometry geometry) {
			if (geometry == null || geometry.IsNull)
				return SqlNumber.Null;

			return geometry.Area;
		}

		public static Field Boundary(Field geometry) {
			var input = (SqlGeometry) geometry.Value;
			var result = input.Boundary;
			return new Field(SpatialType.Geometry(), result);
		}
	}
}
