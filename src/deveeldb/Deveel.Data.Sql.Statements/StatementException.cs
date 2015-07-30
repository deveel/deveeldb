using System;

namespace Deveel.Data.Sql.Statements {
	public class StatementException : SqlErrorException {
		public StatementException() 
			: this(SqlModelErrorCodes.StatementExecute) {
		}

		public StatementException(int errorCode) 
			: base(errorCode) {
		}

		public StatementException(string message) 
			: this(SqlModelErrorCodes.StatementExecute, message) {
		}

		public StatementException(int errorCode, string message) 
			: base(errorCode, message) {
		}

		public StatementException(string message, Exception innerException) 
			: this(SqlModelErrorCodes.StatementExecute, message, innerException) {
		}

		public StatementException(int errorCode, string message, Exception innerException) 
			: base(errorCode, message, innerException) {
		}
	}
}
