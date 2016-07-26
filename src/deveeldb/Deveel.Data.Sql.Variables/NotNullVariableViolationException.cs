using System;

namespace Deveel.Data.Sql.Variables {
	public sealed class NotNullVariableViolationException : ConstraintViolationException {
		internal NotNullVariableViolationException(string variableName)
			: base(SystemErrorCodes.NotNullVariableViolation, FormatMessage(variableName)) {
			VariableName = variableName;
		}

		public string VariableName { get; private set; }

		private static string FormatMessage(string variableName) {
			return String.Format("Attempt to set NULL to the variable '{0}' marked as NOT NULL.", variableName);
		}
	}
}
