using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class SystemFunctionInfo : FunctionInfo {
		public SystemFunctionInfo(string functionName, RoutineParameter[] parameters, FunctionType functionType) 
			: this(functionName, parameters, null, functionType) {
		}

		public SystemFunctionInfo(string functionName, RoutineParameter[] parameters, SqlType returnType, FunctionType functionType) 
			: base(new ObjectName(functionName), parameters, returnType, functionType) {
		}
	}
}
