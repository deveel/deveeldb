using System;
using System.IO;

namespace Deveel.Data.Spatial {
	public sealed class WkbReader : IGeometryReader {
		public WkbReader(IGeometryFactory factory) {
			Factory = factory;
		}

		public IGeometryFactory Factory { get; private set; }

		GeometryFormat IGeometryReader.Format {
			get { return GeometryFormat.WellKnownBinary; }
		}

		IGeometry IGeometryReader.Read(object input) {
			throw new NotImplementedException();
		}

		public IGeometry Read(Stream stream) {
			var reader = new BinaryReader(stream);
			return Read(reader);
		}

		public IGeometry Read(byte[] bytes) {
			using (var stream = new MemoryStream(bytes)) {
				return Read(stream);
			}
		}

		public IGeometry Read(BinaryReader reader) {
			throw new NotImplementedException();
		}
	}
}
