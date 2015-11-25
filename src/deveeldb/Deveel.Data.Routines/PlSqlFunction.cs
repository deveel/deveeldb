using System;

namespace Deveel.Data.Routines {
	public sealed class PlSqlFunction : Function {
		public PlSqlFunction(FunctionInfo functionInfo) 
			: base(functionInfo) {
		}

		public Block Body { get; set; }

		public override ExecuteResult Execute(ExecuteContext context) {
			if (Body == null)
				throw new InvalidOperationException();

			throw new NotImplementedException();
		}
	}
}
