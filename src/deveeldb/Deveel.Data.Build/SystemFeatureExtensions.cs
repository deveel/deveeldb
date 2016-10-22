using System;

namespace Deveel.Data.Build {
	public static class SystemFeatureExtensions {
		public static FeatureInfo GetInfo(this ISystemFeature feature) {
			return FeatureInfo.BuildFrom(feature);
		}
	}
}
