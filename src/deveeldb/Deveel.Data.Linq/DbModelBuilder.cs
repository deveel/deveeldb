using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Linq {
	public sealed class DbModelBuilder {
		private Dictionary<Type, ITypeConfiguration> configurations;

		internal DbModelBuilder(Type contextType) {
			ContextType = contextType;
			configurations = new Dictionary<Type, ITypeConfiguration>();
		}

		private Type ContextType { get; set; }

		public TypeConfiguration<T> Type<T>() where T : class {
			ITypeConfiguration configuration;
			if (!configurations.TryGetValue(typeof(T), out configuration)) {
				configuration = new TypeConfiguration<T>();
				configurations[typeof(T)] = configuration;
			}

			return (TypeConfiguration<T>) configuration;
		}

		public void AddConfiguration<T>(TypeConfiguration<T> configuration) where T : class {
			configurations[typeof(T)] = configuration;
		}

		internal DbCompiledModel Compile() {
			var typeModels = new List<DbTypeModel>();
			foreach (var configuration in configurations.Values) {
				typeModels.Add(configuration.CreateModel());
			}

			return new DbCompiledModel(ContextType, typeModels);
		}
	}
}
