using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class PlSqlFunctionInfo : FunctionInfo {
		public PlSqlFunctionInfo(ObjectName functionName, RoutineParameter[] parameters, SqlType returnType, PlSqlBlockStatement body)
			: base(functionName, parameters,returnType, FunctionType.Static) {
			if (body == null)
				throw new ArgumentNullException("body");

			Body = body;
		}

		public PlSqlBlockStatement Body { get; private set; }
	}
}
