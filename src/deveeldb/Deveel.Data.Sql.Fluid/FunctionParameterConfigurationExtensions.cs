// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Fluid {
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