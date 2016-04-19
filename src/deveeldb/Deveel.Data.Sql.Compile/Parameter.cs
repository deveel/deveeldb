// 
//  Copyright 2010-2016 Deveel
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
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	static class Parameter {
		public static RoutineParameter Routine(PlSqlParser.ParameterContext context) {
			var paramName = Name.Simple(context.parameter_name());
			var paramType = SqlTypeParser.Parse(context.datatype());

			var paramDir = ParameterDirection.Input;
			if (context.IN() != null) {
				if (context.OUT() != null) {
					paramDir = ParameterDirection.InputOutput;
				} else {
					paramDir = ParameterDirection.Input;
				}
			} else if (context.OUT() != null) {
				paramDir = ParameterDirection.Output;
			} else if (context.INOUT() != null) {
				paramDir = ParameterDirection.InputOutput;
			}

			SqlExpression defaultValue = null;
			var defaultPart = context.defaultValuePart();
			if (defaultPart != null) {
				defaultValue = Expression.Build(defaultPart.expression());
			}

			// TODO: Support default in RoutineParameter
			return new RoutineParameter(paramName, paramType, paramDir);
		}
	}
}
