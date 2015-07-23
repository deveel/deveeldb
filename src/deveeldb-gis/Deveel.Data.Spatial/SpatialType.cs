using System;

using Deveel.Data.Types;

namespace Deveel.Data.Spatial {
	public sealed class SpatialType : DataType {
		public SpatialType() 
			: this(-1) {
		}

		public SpatialType(int srid) 
			: base("GEOMETRY", SqlTypeCode.Type) {
			Srid = srid;
		}

		public int Srid { get; private set; }

		public override bool IsComparable(DataType type) {
			var otherType = type as SpatialType;
			if (otherType == null)
				return false;

			return IsComparable(otherType);
		}

		public bool IsComparable(SpatialType other) {
			// TODO: Is this a good assumption?
			return Srid.Equals(other.Srid);
		}

		public static SpatialType Geometry(int srid) {
			return new SpatialType(srid);
		}

		public static SpatialType Geometry() {
			return Geometry(-1);
		}
	}
}
