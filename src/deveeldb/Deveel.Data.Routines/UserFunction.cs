using System;

namespace Deveel.Data.Routines {
	public sealed class UserFunction : Function {
		public UserFunction(FunctionInfo functionInfo) 
			: base(functionInfo) {
			if (functionInfo.FunctionType != FunctionType.UserDefined)
				throw new ArgumentException("The function information are invalid.");
		}

		// TODO: Have a PL/SQL block object

		protected override DataObject Evaluate(DataObject[] args) {
			throw new NotImplementedException();
		}
	}
}
