using System;

namespace Deveel.Data.Routines {
	public sealed class ExternalProcedure : Procedure {
		public ExternalProcedure(ProcedureInfo procedureInfo) : base(procedureInfo) {
		}

		public override InvokeResult Execute(InvokeContext context) {
			throw new NotImplementedException();
		}
	}
}
