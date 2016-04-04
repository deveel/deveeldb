using System;

namespace Deveel.Data.Transactions {
	public enum TransactionState {
		Open = 1,
		Commit = 2,
		Rollback = 3,
		Finished = 4
	}
}
