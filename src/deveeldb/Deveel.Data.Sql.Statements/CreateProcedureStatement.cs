using System;
using System.Collections.Generic;

using Deveel.Data.Routines;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateProcedureStatement : SqlStatement {
		public CreateProcedureStatement(ObjectName procedureName, PlSqlBlockStatement body) 
			: this(procedureName, null, body) {
		}

		public CreateProcedureStatement(ObjectName procedureName, IEnumerable<RoutineParameter> parameters, PlSqlBlockStatement body) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");
			if (body == null)
				throw new ArgumentNullException("body");

			ProcedureName = procedureName;
			Parameters = parameters;
			Body = body;
		}

		public ObjectName ProcedureName { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public PlSqlBlockStatement Body { get; private set; }
	}
}
