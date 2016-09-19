using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Mapping {
	public sealed class TypeConfigurationRegistry : IDisposable {
		private Dictionary<Type, ITypeConfigurationProvider> configurations;

		internal TypeConfigurationRegistry() {
			configurations = new Dictionary<Type, ITypeConfigurationProvider>();
		}

		~TypeConfigurationRegistry() {
			Dispose(false);
		}

		internal IEnumerable<ITypeConfigurationProvider> Configurations {
			get { return configurations.Values; }
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (configurations != null)
					configurations.Clear();
			}

			configurations = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public void Add<TType>(TypeConfiguration<TType> configuration) where TType : class {
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			if (configurations.ContainsKey(typeof(TType)))
				throw new ArgumentException(String.Format("A configuration for type '{0}' is already registered.", typeof(TType)));

			configurations[typeof(TType)] = configuration;
		}

		public void AddFromAssembly(Assembly assembly) {
			throw new NotImplementedException();
		}
	}
}
