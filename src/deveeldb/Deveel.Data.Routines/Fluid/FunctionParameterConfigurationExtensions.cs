using System;

using Deveel.Data.Types;

namespace Deveel.Data.Routines.Fluid {
	public static class FunctionParameterConfigurationExtensions {
		public static IFunctionParameterConfiguration Unbounded(this IFunctionParameterConfiguration config) {
			return config.Unbounded(true);
		}

		public static IFunctionParameterConfiguration OfVarCharType(this IFunctionParameterConfiguration config, int maxSize) {
			return config.OfType(PrimitiveTypes.String(SqlTypeCode.VarChar, maxSize));
		}

		public static IFunctionParameterConfiguration OfVarCharType(this IFunctionParameterConfiguration config) {
			return config.OfType(PrimitiveTypes.String(SqlTypeCode.VarChar));
		}

		public static IFunctionParameterConfiguration OfStringType(this IFunctionParameterConfiguration config) {
			return config.OfType(PrimitiveTypes.String());
		}

		public static IFunctionParameterConfiguration OfNumericType(this IFunctionParameterConfiguration config, int size) {
			return config.OfType(PrimitiveTypes.Numeric(size));
		}

		public static IFunctionParameterConfiguration OfNumericType(this IFunctionParameterConfiguration config) {
			return config.OfType(PrimitiveTypes.Numeric());
		}

		public static IFunctionParameterConfiguration OfDynamicType(this IFunctionParameterConfiguration config) {
			return config.OfType(Function.DynamicType);
		}

		// TODO: More types ...
	}
}