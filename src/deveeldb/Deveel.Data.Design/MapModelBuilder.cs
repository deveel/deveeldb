using System;

namespace Deveel.Data.Design {
	public sealed class MapModelBuilder {
		internal MapModelBuilder() {
			Configurations = new TypeConfigurationRegistry();
		}
		public TypeConfigurationRegistry Configurations { get; private set; }

		public TypeConfiguration<TType> Type<TType>() where TType : class {
			return Configurations.GetOrAdd<TType>();
		}

		internal CompiledModel CompileModel() {
			var map = new CompiledModel();
			foreach (var configuration in Configurations.Configurations) {
				map.AddConfiguration(configuration);
			}

			return map;
		}
	}
}
