using System;
using System.IO;

using Deveel.Data.DbSystem;
using Deveel.Data.Spatial;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public class GeometryType : DataType {
		public GeometryType() 
			: this(-1) {
		}

		public GeometryType(int srid) 
			: base(SqlTypeCode.Geometry) {
			Srid = srid;
		}

		public int Srid { get; private set; }

		public override bool Equals(DataType other) {
			var geomType = other as GeometryType;
			if (geomType == null)
				return false;

			return Srid.Equals(geomType.Srid);
		}

		public override SqlBoolean IsEqualTo(ISqlObject a, ISqlObject b) {
			var g1 = (IGeometry) a;
			var g2 = (IGeometry) b;

			if (g1.Srid != g2.Srid)
				return false;

			return g1.EqualsExact(g2);
		}

		public override bool IsComparable(DataType type) {
			var geomType = type as GeometryType;
			if (geomType == null)
				return false;

			return Srid.Equals(geomType.Srid);
		}

		public override int Compare(ISqlObject x, ISqlObject y) {
			var g1 = (IGeometry)x;
			var g2 = (IGeometry)y;

			return g1.CompareTo(g2);
		}

		public override bool IsIndexable {
			get { return false; }
		}

		public override ISqlObject Reverse(ISqlObject value) {
			var g = (IGeometry) value;
			return g.Reverse();
		}

		public override void Serialize(Stream stream, ISqlObject obj) {
			var geometry = (IGeometry) obj;

			var writer = new BinaryWriter(stream);
			var wkb = new WkbWriter();
			var bytes = wkb.Write(geometry);

			writer.Write(bytes.Length);
			writer.Write(bytes);
		}

		public override ISqlObject Deserialize(Stream stream, ISystemContext context) {
			var reader = new BinaryReader(stream);

			var length = reader.ReadInt32();
			var bytes = reader.ReadBytes(length);

			var factory = context.SpatialContext.GeometryFactory;
			var wkbReader = new WkbReader(factory);
			return wkbReader.Read(bytes);
		}
	}
}
