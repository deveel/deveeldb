using System;

namespace Deveel.Data.Transactions {
	public enum TransactionEventType {
		Begin = 1,
		Commit = 2,
		Rollback = 3
	}
}
