using System;

using Deveel.Data.Types;

namespace Deveel.Data.Routines.Fluid {
	public static class FunctionConfigurationExtensions {
		public static IFunctionConfiguration Named(this IFunctionConfiguration configuration, string name) {
			var routineConfig = configuration as IRoutineConfiguration;
			if (routineConfig == null)
				throw new InvalidOperationException();

			return configuration.Named(new ObjectName(routineConfig.Context.SchemaName, name));
		}

		public static IFunctionConfiguration WithAlias(this IFunctionConfiguration configuration, string alias) {
			var routineConfig = configuration as IRoutineConfiguration;
			if (routineConfig == null)
				throw new InvalidOperationException();

			return configuration.WithAlias(new ObjectName(routineConfig.Context.SchemaName, alias));
		}

		public static IFunctionConfiguration WithParameter(this IFunctionConfiguration configuration, string name,
			DataType type) {
			return configuration.WithParameter(config => config.Named(name).OfType(type));
		}

		public static IFunctionConfiguration WithUnoundedParameter(this IFunctionConfiguration configuration, string name,
			DataType type) {
			return configuration.WithParameter(config => config.Named(name).OfType(type).Unbounded());
		}

		public static IFunctionConfiguration ReturnsType(this IFunctionConfiguration configuration, DataType type) {
			return configuration.ReturnsType(context => type);
		}
	}
}