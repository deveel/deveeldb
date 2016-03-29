using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class ExternalFunctionInfo : FunctionInfo {
		public ExternalFunctionInfo(ObjectName functionName, RoutineParameter[] parameters, SqlType returnType,
			ExternalRef externalRef) 
			: base(functionName, parameters, returnType, FunctionType.Static) {
			if (externalRef == null)
				throw new ArgumentNullException("externalRef");

			ExternalRef = externalRef;
		}

		public ExternalRef ExternalRef { get; private set; }
	}
}
