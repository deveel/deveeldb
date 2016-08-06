using System;

namespace Deveel.Data.Transactions {
	public enum LockEventType {
		/// <summary>
		/// A temporary lock was acquired on a a set of resources
		/// </summary>
		Enter = 1,

		/// <summary>
		/// A temporary lock was released from a set of resources
		/// </summary>
		Exit = 2,

		/// <summary>
		/// An explicit locking happened over a given set of resource
		/// </summary>
		Lock = 3,

		/// <summary>
		/// An existing explicit lock was released (manually or from the expiration
		/// of the transaction that acquired the lock).
		/// </summary>
		Release = 4
	}
}
