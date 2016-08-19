using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class LockTimeoutException : TransactionException {
		internal LockTimeoutException(ObjectName tableName, AccessType accessType, int timeout)
			: base(SystemErrorCodes.LockTimeout, FormatMessage(tableName, accessType, timeout)) {
			TableName = tableName;
			AccessType = accessType;
			Timeout = timeout;
		}

		public ObjectName TableName { get; private set; }

		public int Timeout { get; private set; }

		public AccessType AccessType { get; private set; }

		public static string FormatMessage(ObjectName tableName, AccessType accessType, int timeout) {
			var timeoutString = timeout == System.Threading.Timeout.Infinite
				? "Infinite"
				: String.Format("{0}ms", timeout);
			var accessTypeString = accessType == AccessType.ReadWrite
				? "read/write"
				: accessType.ToString().ToLowerInvariant();
			return String.Format("A {0} lock on {1} was not released before {2}", accessTypeString, tableName, timeoutString);
		}
	}
}
