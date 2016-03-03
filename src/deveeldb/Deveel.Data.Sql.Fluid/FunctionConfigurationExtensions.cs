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
	public static class FunctionConfigurationExtensions {
		public static IFunctionConfiguration Named(this IFunctionConfiguration configuration, string name) {
			var routineConfig = configuration as IRoutineConfiguration;
			if (routineConfig == null)
				throw new InvalidOperationException();

			return configuration.Named(new ObjectName(new ObjectName(routineConfig.Context.SchemaName), name));
		}

		public static IFunctionConfiguration WithAlias(this IFunctionConfiguration configuration, string alias) {
			var routineConfig = configuration as IRoutineConfiguration;
			if (routineConfig == null)
				throw new InvalidOperationException();

			return configuration.WithAlias(new ObjectName(new ObjectName(routineConfig.Context.SchemaName), alias));
		}

		public static IFunctionConfiguration OfAggregateType(this IFunctionConfiguration configuration) {
			configuration.OfType(FunctionType.Aggregate);
			return configuration;
		}

		public static IFunctionConfiguration WithParameter(this IFunctionConfiguration configuration, string name,
			SqlType type) {
			return configuration.WithParameter(config => config.Named(name).OfType(type));
		}

		public static IFunctionConfiguration WithNumericParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, PrimitiveTypes.Numeric());
		}

		public static IFunctionConfiguration WithStringParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, PrimitiveTypes.String());
		}

		public static IFunctionConfiguration WithDynamicParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, Function.DynamicType);
		}

		public static IFunctionConfiguration WithBooleanParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, PrimitiveTypes.Boolean());
		}

		public static IFunctionConfiguration WithDateTimeParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, PrimitiveTypes.DateTime());
		}

		public static IFunctionConfiguration WithUnoundedParameter(this IFunctionConfiguration configuration, string name,
			SqlType type) {
			return configuration.WithParameter(config => config.Named(name).OfType(type).Unbounded());
		}

		public static IFunctionConfiguration ReturnsType(this IFunctionConfiguration configuration, SqlType type) {
			return configuration.ReturnsType(context => type);
		}

		public static IFunctionConfiguration ReturnsString(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(PrimitiveTypes.String());
		}

		public static IFunctionConfiguration ReturnsNumeric(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(PrimitiveTypes.Numeric());
		}

		public static IFunctionConfiguration ReturnsBoolean(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(PrimitiveTypes.Boolean());
		}

		public static IFunctionConfiguration ReturnsBinary(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(PrimitiveTypes.Binary());
		}

		public static IFunctionConfiguration ReturnsDateTime(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(PrimitiveTypes.DateTime());
		}
	}
}