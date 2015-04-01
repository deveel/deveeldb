using System;

namespace Deveel.Data.Spatial {
	public sealed class WkbWriter : IGeometryWriter {
		GeometryFormat IGeometryWriter.Format {
			get { return GeometryFormat.WellKnownBinary; }
		}

		object IGeometryWriter.Write(IGeometry geometry) {
			return Write(geometry);
		}

		public byte[] Write(IGeometry geometry) {
			throw new NotImplementedException();
		}
	}
}
