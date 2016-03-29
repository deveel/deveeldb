using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Routines {
	public sealed class PlSqlProcedureInfo : ProcedureInfo {
		public PlSqlProcedureInfo(ObjectName procedureName, RoutineParameter[] parameters, PlSqlBlockStatement body)
			: base(procedureName, parameters) {
			if (body == null)
				throw new ArgumentNullException("body");

			Body = body;
		}

		public PlSqlBlockStatement Body { get; private set; }
	}
}
