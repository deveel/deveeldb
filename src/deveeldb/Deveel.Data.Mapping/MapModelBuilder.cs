using System;

namespace Deveel.Data.Mapping {
	public sealed class MapModelBuilder {
		internal MapModelBuilder() {
			Configurations = new TypeConfigurationRegistry();
		}
		public TypeConfigurationRegistry Configurations { get; private set; }

		internal CompiledModel CompileModel() {
			var map = new CompiledModel();
			foreach (var configuration in Configurations.Configurations) {
				map.AddConfiguration(configuration);
			}

			return map;
		}
	}
}
