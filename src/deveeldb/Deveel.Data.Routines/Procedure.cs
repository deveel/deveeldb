using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	public abstract class Procedure : IProcedure {
		protected Procedure(ProcedureInfo procedureInfo) {
			if (procedureInfo == null)
				throw new ArgumentNullException("procedureInfo");

			ProcedureInfo = procedureInfo;
		}

		public ProcedureInfo ProcedureInfo { get; private set; }

		public ObjectName ProcedureName {
			get { return ProcedureInfo.RoutineName; }
		}

		ObjectName IDbObject.FullName {
			get { return ProcedureInfo.RoutineName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Routine; }
		}

		RoutineType IRoutine.Type {
			get { return RoutineType.Procedure; }
		}

		RoutineInfo IRoutine.RoutineInfo {
			get { return ProcedureInfo; }
		}

		public abstract InvokeResult Execute(InvokeContext context);
	}
}
