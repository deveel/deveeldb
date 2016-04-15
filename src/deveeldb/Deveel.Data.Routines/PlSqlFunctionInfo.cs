using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class PlSqlFunctionInfo : FunctionInfo {
		public PlSqlFunctionInfo(ObjectName functionName, RoutineParameter[] parameters, SqlType returnType, SqlStatement body)
			: base(functionName, parameters,returnType, FunctionType.Static) {
			if (body == null)
				throw new ArgumentNullException("body");

			// TODO: in case of RETURNS TABLE verify a select is there
			if (!(returnType is TabularType)) {
				if (!ReturnChecker.HasReturn(body))
					throw new ArgumentException("The function body has no return");
			}

			Body = body;
		}

		public SqlStatement Body { get; private set; }
	}
}
