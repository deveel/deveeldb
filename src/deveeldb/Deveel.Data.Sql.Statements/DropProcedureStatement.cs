using System;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropProcedureStatement : SqlStatement {
		public DropProcedureStatement(ObjectName procedureName) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");

			ProcedureName = procedureName;
		}

		public ObjectName ProcedureName { get; private set; }

		public bool IfExists { get; set; }
	}
}
