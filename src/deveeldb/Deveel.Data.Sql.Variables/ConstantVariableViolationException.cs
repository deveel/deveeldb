using System;

namespace Deveel.Data.Sql.Variables {
	public sealed class ConstantVariableViolationException : ConstraintViolationException {
		internal ConstantVariableViolationException(string variableName)
			: base(SystemErrorCodes.ConstantVariableViolation, FormatMessage(variableName)) {
			VariableName = variableName;
		}

		public string VariableName { get; private set; }

		private static string FormatMessage(string variableName) {
			return String.Format("Attempt to set a value to the variable '{0}' marked as CONSTANT", variableName);
		}
	}
}
