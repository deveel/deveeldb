// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.Generic;

namespace Deveel.Data.Transactions {
	public sealed class LockHandle : IDisposable {
		private bool released;
		private List<Lock> locks;
		private readonly Locker locker;

		internal LockHandle(Locker locker) {
			this.locker = locker;
			locks = new List<Lock>();
		}

		~LockHandle() {
			Dispose(false);
		}

		internal void AddLock(Lock @lock) {
			locks.Add(@lock);
		}

		internal void ReleaseLocks() {
			if (!released) {
				for (int i = locks.Count - 1; i >= 0; i--) {
					locks[i].Release();
				}

				locks.Clear();
				released = true;
			}
		}

		public void Wait(ILockable lockable, AccessType accessType) {
			//TODO: var timeout = locker.context.LockTimeout();
			var timeout = 300;

			Wait(lockable, accessType, timeout);
		}

		public void Wait(ILockable lockable, AccessType accessType, int timeout) {
			bool found = false;
			for (int i = locks.Count - 1; i >= 0; i--) {
				var @lock = locks[i];
				if (@lock.Locked.RefId.Equals(lockable.RefId) &&
					(@lock.AccessType & accessType) != 0) {
					@lock.Wait(accessType, timeout);
					found = true;
				}
			}

			if (!found)
				throw new InvalidOperationException("The handle does not lock the object trying to access");

		}

		public void WaitAll(int timeout) {
			for (int i = locks.Count - 1; i >= 0; i--) {
				locks[i].Wait(locks[i].AccessType, timeout);
			}
		}

		public void WaitAll() {
			//TODO: var timeout = locker.context.LockTimeout();
			var timeout = 300;

			WaitAll(timeout);
		}

		public void Release() {
			locker.Release(this);
		}

		public bool IsHandled(ILockable lockable) {
			for (int i = locks.Count - 1; i >= 0; i--) {
				if (locks[i].Locked.RefId.Equals(lockable.RefId))
					return true;
			}

			return false;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (!released)
					Release();
			}
		}
	}
}