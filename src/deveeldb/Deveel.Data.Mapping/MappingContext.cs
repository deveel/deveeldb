using System;

namespace Deveel.Data.Mapping {
	public abstract class MappingContext : IMappingContext {
		void IMappingContext.OnBuildMap(MapModelBuilder builder) {
			OnBuildMap(builder);
		}

		protected abstract void OnBuildMap(MapModelBuilder builder);
	}
}
