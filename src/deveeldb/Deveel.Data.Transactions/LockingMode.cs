using System;

namespace Deveel.Data.Transactions {
	public enum LockingMode {
		None = 0,
		Exclusive = 1,
		Shared = 2
	}
}