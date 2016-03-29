using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public abstract class SystemFunction : Function, ISystemFunction {
		protected SystemFunction(SystemFunctionInfo functionInfo) 
			: base(functionInfo) {
		}

		protected SystemFunction(string name, RoutineParameter[] parameters, FunctionType functionType)
			: this(name, parameters, null, functionType) {
		}

		protected SystemFunction(string name, RoutineParameter[] parameters, SqlType returnType, FunctionType functionType)
			: this(new SystemFunctionInfo(name, parameters, returnType, functionType)) {
			
		}
	}
}
