using System;

namespace Deveel.Data.Spatial {
	public interface IGeometryWriterResolver {
		IGeometryWriter ResolveWriter(GeometryFormat format);
	}
}
