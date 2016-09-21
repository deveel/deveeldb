using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Design.Configuration {
	public sealed class TypeConfigurationRegistry {
		private ModelConfiguration modelConfiguration;

		internal TypeConfigurationRegistry(ModelConfiguration modelConfiguration) {
			this.modelConfiguration = modelConfiguration;
		}

		public void Add<TType>(TypeConfiguration<TType> configuration) where TType : class {
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			configuration.AttachTo(modelConfiguration);
		}

		public void AddFromAssembly(Assembly assembly) {
			throw new NotImplementedException();
		}

		internal TypeConfiguration<TType> GetOrAdd<TType>() where TType : class {
			return new TypeConfiguration<TType>(modelConfiguration);
		}
	}
}
