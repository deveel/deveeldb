using System;
using System.Collections.Generic;

using Deveel.Data.Routines;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateExternalProcedureStatement : SqlStatement {
		public CreateExternalProcedureStatement(ObjectName procedureName, string externalReference) 
			: this(procedureName, null, externalReference) {
		}

		public CreateExternalProcedureStatement(ObjectName procedureName, IEnumerable<RoutineParameter> parameters, string externalRef) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");
			if (String.IsNullOrEmpty(externalRef))
				throw new ArgumentNullException("externalRef");

			ProcedureName = procedureName;
			Parameters = parameters;
			ExternalReference = externalRef;
		}

		public ObjectName ProcedureName { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public string ExternalReference { get; private set; }
	}
}
