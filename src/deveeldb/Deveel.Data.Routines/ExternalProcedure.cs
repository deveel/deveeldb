using System;

namespace Deveel.Data.Routines {
	public sealed class ExternalProcedure : Procedure {
		public ExternalProcedure(ProcedureInfo procedureInfo) 
			: base(procedureInfo) {
			if (procedureInfo.ExternalRef == null)
				throw new ArgumentNullException("procedureInfo", "The procedure info has no external reference specified.");

			procedureInfo.ExternalRef.CheckReference(procedureInfo);
		}

		public override InvokeResult Execute(InvokeContext context) {
			throw new NotImplementedException();
		}
	}
}
