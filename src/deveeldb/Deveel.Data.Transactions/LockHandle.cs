// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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

		internal void CheckAccess(ILockable lockable, AccessType accessType, int timeout) {
			if (accessType == AccessType.ReadWrite) {
				CheckAccess(lockable, AccessType.Read, timeout);
				CheckAccess(lockable, AccessType.Write, timeout);
				return;
			}

			for (int i = locks.Length - 1; i >= 0; --i) {
				var @lock = locks[i];
				if (@lock.Lockable.RefId.Equals(lockable.RefId) &&
					@lock.AccessType == accessType) {
					@lock.CheckAccess(accessType, timeout);
					return;
				}
			}

			throw new Exception("The given object was not found in the lock list for this handle");
		}

		public bool IsHandled(ILockable lockable) {
			for (int i = locks.Length - 1; i >= 0; i--) {
				if (locks[i].Lockable == lockable)
					return true;
			}

			return false;
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