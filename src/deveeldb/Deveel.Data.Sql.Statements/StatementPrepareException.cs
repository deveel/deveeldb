using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// An exception that happens during the <see cref="SqlStatement.Prepare"/>.
	/// </summary>
	public class StatementPrepareException : SqlErrorException {
		public StatementPrepareException() 
			: this(SqlModelErrorCodes.StatementPrepare) {
		}

		public StatementPrepareException(int errorCode) 
			: base(errorCode) {
		}

		public StatementPrepareException(string message) 
			: this(SqlModelErrorCodes.StatementPrepare, message) {
		}

		public StatementPrepareException(int errorCode, string message) 
			: base(errorCode, message) {
		}

		public StatementPrepareException(string message, Exception innerException) 
			: this(SqlModelErrorCodes.StatementPrepare, message, innerException) {
		}

		public StatementPrepareException(int errorCode, string message, Exception innerException) 
			: base(errorCode, message, innerException) {
		}
	}
}
