using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	public sealed class ExternalProcedureInfo : ProcedureInfo {
		public ExternalProcedureInfo(ObjectName procedureName, RoutineParameter[] parameters, ExternalRef externalRef)
			: base(procedureName, parameters) {
			if (externalRef == null)
				throw new ArgumentNullException("externalRef");

			ExternalRef = externalRef;
		}

		public ExternalRef ExternalRef { get; private set; }
	}
}
