using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class NonCommittedConflictException : TransactionException {
		internal NonCommittedConflictException(ObjectName objectName)
			: base(SystemErrorCodes.NonCommittedConflict, String.Format("Object '{0}' is not a commit resource.", objectName)) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }
	}
}
