namespace Deveel.Data.Transactions {
	public enum IsolationLevel {
		Unspecified = 0,
		Serializable = 1,
		ReadCommitted = 2,
		ReadUncommitted = 3,
		Snapshot = 4
	}
}