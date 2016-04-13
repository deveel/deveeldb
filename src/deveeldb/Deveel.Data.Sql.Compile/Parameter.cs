using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	static class Parameter {
		public static RoutineParameter Routine(PlSqlParser.ParameterContext context) {
			var paramName = Name.Simple(context.parameter_name());
			var paramType = SqlTypeParser.Parse(context.type_spec());

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
