using System;
using System.Collections.Generic;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class MappingContext {
		private Dictionary<Type, ITypeMappingConfiguration> configurations;

		public MappingContext() {
			TableNameConvention = RuledNamingConvention.SqlNaming;
			ColumnNameConvention = RuledNamingConvention.SqlNaming;
		}

		public INamingConvention TableNameConvention { get; set; }

		public INamingConvention ColumnNameConvention { get; set; }

		public TypeMappingConfiguration<T> Map<T>() where T : class {
			return Map<T>(null);
		}

		public TypeMappingConfiguration<T> Map<T>(TypeMappingConfiguration<T> configuration) where T : class {
			if (configurations == null)
				configurations = new Dictionary<Type, ITypeMappingConfiguration>();

			var type = typeof (T);

			if (configuration == null) {
				ITypeMappingConfiguration config;
				if (!configurations.TryGetValue(type, out config)) {
					configuration = new TypeMappingConfiguration<T>();
					configurations[type] = configuration;
				} else {
					configuration = (TypeMappingConfiguration<T>) config;
				}
			} else {
				configurations[type] = configuration;
			}

			return configuration;
		}

		public MappingModel CreateModel(IQueryContext queryContext) {
			throw new NotImplementedException();
		}
	}
}
