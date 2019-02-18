using System;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements.Security {
	public sealed class SqlStatementSecurityException : SecurityException {
		public SqlStatementSecurityException(string userName)
			: this(userName, $"User '{userName}' has not enough privileges to execute the statement") {
		}

		public SqlStatementSecurityException(string userName, string message)
			: base(message) {
			UserName = userName;
		}

		public string UserName { get; }
	}
}