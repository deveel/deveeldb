using System;

namespace Deveel.Data.Transactions {
	public sealed class ReadOnlyTransactionException : TransactionException {
		internal ReadOnlyTransactionException(int commidId)
			: base(SystemErrorCodes.ReadOnlyTransaction, String.Format("Transaction '{0}' is read-only", commidId)) {
			CommitId = commidId;
		}

		public int CommitId { get; private set; }
	}
}
