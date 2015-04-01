using System;

namespace Deveel.Data.Spatial {
	public interface IGeometryReaderResolver {
		IGeometryReader ResolveReader(GeometryFormat format);
	}
}
