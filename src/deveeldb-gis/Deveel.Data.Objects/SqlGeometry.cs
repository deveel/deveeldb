using System;

using Deveel.Data.Sql.Objects;

using GeoAPI.Geometries;

namespace Deveel.Data.Objects {
	public class SqlGeometry : ISqlObject {
		public static SqlGeometry Null = new SqlGeometry(null);

		internal SqlGeometry(IGeometry geometry) {
			if (geometry == null)
				throw new ArgumentNullException("geometry");

			Geometry = geometry;
		}

		internal IGeometry Geometry { get; private set; }

		int IComparable.CompareTo(object obj) {
			if (!(obj is SqlGeometry))
				throw new ArgumentException();

			return CompareTo((SqlGeometry) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			if (!(other is SqlGeometry))
				throw new ArgumentException();

			return CompareTo((SqlGeometry)other);
		}

		public int CompareTo(SqlGeometry other) {
			return Geometry.CompareTo(other.Geometry);
		}

		public bool IsNull {
			get { return Geometry == null; }
		}

		public SqlGeometry Boundary {
			get {
				if (IsNull)
					return Null;

				return new SqlGeometry(Geometry.Boundary);
			}
		}

		public SqlGeometry Envelope {
			get {
				if (IsNull)
					return Null;

				return new SqlGeometry(Geometry.Envelope);
			}
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			if (!(other is SqlGeometry))
				return false;

			return IsComparableTo((SqlGeometry)other);
		}

		public bool IsComparableTo(SqlGeometry other) {
			if (IsNull && (other == null || other.IsNull))
				return true;
			if (!IsNull && (other != null && !other.IsNull))
				return false;

			return true;
		}
	}
}
