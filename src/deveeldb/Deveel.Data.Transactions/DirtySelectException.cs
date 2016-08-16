using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class DirtySelectException : TransactionException {
		internal DirtySelectException(ObjectName tableName)
			: base(SystemErrorCodes.DirtySelectInTransaction, String.Format("Selection from table '{0}' that was modified.", tableName)) {
			TableName = tableName;
		}

		public ObjectName TableName { get; private set; }
	}
}
