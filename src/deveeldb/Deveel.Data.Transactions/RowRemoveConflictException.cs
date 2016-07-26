using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Transactions {
	public sealed class RowRemoveConflictException : TransactionException {
		internal RowRemoveConflictException(ObjectName tableName, RowId rowId)
			: base(SystemErrorCodes.RowRemoveConflict,
				String.Format("Row '{0}' in table '{1}' was removed twice in the same transaction", rowId, tableName)) {
		}
	}
}
