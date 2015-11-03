using System;

using Deveel.Data.Sql.Fluid;

namespace Deveel.Data.Spatial {
	static class FluentConfigurationExtensions {
		public static IFunctionConfiguration ReturnsSpatialType(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(SpatialType.Geometry());
		}

		public static IFunctionConfiguration WithSpatialParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, SpatialType.Geometry());
		}
	}
}
