using System;

namespace Deveel.Data.Sql.Expressions {
	public sealed class ExpressionFormatException : SqlExpressionException {
		internal ExpressionFormatException(string message, Exception innerException)
			: base(SystemErrorCodes.InvalidExpressionFormat, message, innerException) {
		}
	}
}
