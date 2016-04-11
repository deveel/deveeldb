using System;

namespace Deveel.Data.Mapping {
	public class MapContext {
		protected virtual void OnBuild(MapBuilder builder) {
		}

		internal CompiledMap Compile() {
			var builder = new MapBuilder();
			OnBuild(builder);

			return builder.Compile();
		}
	}
}
