using System;
using System.IO;

using Deveel.Data.Sql.Objects;

using GeoAPI;
using GeoAPI.Geometries;

using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Deveel.Data.Objects {
	public class SqlGeometry : ISqlObject {
		public static SqlGeometry Null = new SqlGeometry(null, true);

		// TODO: Make these ones configurable and in context...
		private static readonly IGeometryServices DefaultGeometryServices = new NtsGeometryServices();
		private static readonly IGeometryFactory DefaultGeometryFactory = new GeometryFactory();


		private SqlGeometry(IGeometry geometry, bool isNull) {
			Geometry = geometry;
			IsNull = isNull;
		}

		private SqlGeometry(IGeometry geometry)
			: this(geometry, false) {
			if (geometry == null)
				throw new ArgumentNullException("geometry");
		}

		private IGeometry Geometry { get; set; }

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

		public bool IsNull { get; private set; }

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

		public SqlGeometry Buffer(SqlNumber distance) {
			if (distance.IsNull)
				return Null;

			return Buffer(distance.ToDouble());
		}

		public SqlGeometry Buffer(double distance) {
			var result = Geometry.Buffer(distance);
			return new SqlGeometry(result);
		}

		public SqlNumber Distance(SqlGeometry geometry) {
			if (IsNull || (geometry == null || geometry.IsNull))
				return SqlNumber.Null;

			var result = Geometry.Distance(geometry.Geometry);
			return new SqlNumber(result);
		}

		public SqlBoolean Contains(SqlGeometry other) {
			if (IsNull || (other == null || other.IsNull))
				return SqlBoolean.Null;

			return Geometry.Contains(other.Geometry);;
		}

		public SqlBinary ToBinary() {
			if (IsNull)
				return SqlBinary.Null;

			var bytes = Geometry.AsBinary();
			return new SqlBinary(bytes);
		}

		public override string ToString() {
			if (IsNull)
				return String.Empty;

			return ToSqlString().ToString();
		}

		public SqlString ToSqlString() {
			if (IsNull)
				return SqlString.Null;

			var text = Geometry.AsText();
			return new SqlString(text);
		}

		private static bool TryParse(string text, out SqlGeometry geometry, out Exception error) {
			if (String.IsNullOrEmpty(text)) {
				geometry = Null;
				error = new ArgumentNullException("text");
				return false;
			}

			try {
				var reader = new WKTReader(DefaultGeometryFactory);

				IGeometry g;
				using (var textReader = new StringReader(text)) {
					g = reader.Read(textReader);
				}

				geometry = new SqlGeometry(g);
				error = null;
				return true;
			} catch (Exception ex) {
				geometry = Null;
				error = ex;
				return false;
			}
		}

		public static bool TryParse(string text, out SqlGeometry geometry) {
			Exception error;
			return TryParse(text, out geometry, out error);
		}

		public static SqlGeometry Parse(string text) {
			Exception error;
			SqlGeometry geometry;
			if (!TryParse(text, out geometry, out error))
				throw new FormatException(String.Format("Could not parse the input string '{0}' to a valid GEOMETRY.", text), error);

			return geometry;
		}

		public static bool TryParse(byte[] bytes, out SqlGeometry geometry) {
			Exception error;
			return TryParse(bytes, out geometry, out error);
		}

		private static bool TryParse(byte[] bytes, out SqlGeometry geometry, out Exception error) {
			if (bytes == null) {
				geometry = Null;
				error = new ArgumentNullException("bytes");
				return false;
			}

			IGeometry g;

			try {
				var reader = new WKBReader(DefaultGeometryServices);
				using (var stream = new MemoryStream(bytes)) {
					g = reader.Read(stream);
				}

				geometry = new SqlGeometry(g);
				error = null;
				return true;
			} catch (Exception ex) {
				error = ex;
				geometry = Null;
				return false;
			}
		}

		public static SqlGeometry Parse(byte[] bytes) {
			Exception error;
			SqlGeometry geometry;
			if (!TryParse(bytes, out geometry, out error))
				throw new FormatException("Could not parse the input bytes to a valid GEOMETRY.", error);

			return geometry;
		}
	}
}
