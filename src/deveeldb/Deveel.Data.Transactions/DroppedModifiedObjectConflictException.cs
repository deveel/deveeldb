using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class DroppedModifiedObjectConflictException : TransactionException {
		internal DroppedModifiedObjectConflictException(ObjectName objectName)
			: base(SystemErrorCodes.DroppedModifiedObjectConflict, String.Format("Object '{0}' was modified and dropped.", objectName)) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }
	}
}
