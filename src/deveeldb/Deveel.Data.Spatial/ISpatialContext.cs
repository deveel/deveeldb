using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Spatial {
	public interface ISpatialContext : IDatabaseService {
		IGeometryFactory GeometryFactory { get; }

		IGeometryReaderResolver ReaderResolver { get; }

		IGeometryWriterResolver WriterResolver { get; }
	}
}
