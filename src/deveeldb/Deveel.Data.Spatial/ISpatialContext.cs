using System;

namespace Deveel.Data.Spatial {
	public interface ISpatialContext {
		IGeometryFactory GeometryFactory { get; }

		IGeometryReaderResolver ReaderResolver { get; }

		IGeometryWriterResolver WriterResolver { get; }
	}
}
