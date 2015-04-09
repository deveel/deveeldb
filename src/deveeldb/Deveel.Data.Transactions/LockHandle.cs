using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class LockHandle : IDisposable {
		private Lock[] locks;
		private int lockIndex;

		internal LockHandle(int lockCount) {
			locks = new Lock[lockCount];
			lockIndex = 0;
			IsUnlocked = false;
		}

		private bool IsUnlocked { get; set; }

		internal void AddLock(Lock @lock) {
			locks[lockIndex++] = @lock;
		}

		internal void Release() {
			if (!IsUnlocked) {
				for (int i = locks.Length - 1; i >= 0; --i) {
					locks[i].Release();
				}

				IsUnlocked = true;
			}
		}

		public void CheckAccess(ITable table, AccessType accessType) {
			for (int i = locks.Length - 1; i >= 0; --i) {
				var tableLock = locks[i];
				if (tableLock.Lockable == table) {
					tableLock.CheckAccess(accessType);
					return;
				}
			}

			throw new Exception("The given table was not found in the lock list for this handle");
		}

		public void Dispose() {
			if (!IsUnlocked) {
				Release();

				// TODO: report the situation: there should not be
				//        a call to Release() at this point...

				locks = null;
			}
		}
	}
}