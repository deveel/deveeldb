using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class ObjectDuplicatedConflictException : TransactionException {
		internal ObjectDuplicatedConflictException(ObjectName objectName, string conflictType)
			: base(SystemErrorCodes.DuplicateObjectConflict,
				String.Format("The object '{0}' was {1} twice in transaction.", objectName, conflictType)) {
			ObjectName = objectName;
			ConflictType = conflictType;
		}

		public ObjectName ObjectName { get; private set; }

		public string ConflictType { get; private set; }
	}
}
