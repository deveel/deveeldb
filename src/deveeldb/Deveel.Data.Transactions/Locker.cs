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
using System.Collections.Generic;

namespace Deveel.Data.Transactions {
	public sealed class Locker {
		private readonly Dictionary<object, LockingQueue> queuesMap = new Dictionary<object, LockingQueue>();

		public Locker(IDatabase database) {
			Database = database;
		}

		public IDatabase Database { get; private set; }

		private void AddToHandle(LockHandle handle, ILockable[] lockables, AccessType accessType, LockingMode mode) {
			if (lockables == null)
				return;

			for (int i = lockables.Length - 1; i >= 0; --i) {
				var lockable = lockables[i];
				var queue = GetQueueFor(lockable);

				// slightly confusing: this will add Lock to given table queue
				var @lock = new Lock(queue, mode, accessType);
				@lock.Acquire();
				handle.AddLock(@lock);
			}
		}

		public LockHandle Lock(ILockable[] lockables, AccessType accessType, LockingMode mode) {
			lock (this) {
				int count = 0;
				if ((accessType & AccessType.Read) != 0)
					count += lockables.Length;
				if ((accessType & AccessType.Write) != 0)
					count += lockables.Length;

				var handle = new LockHandle(count);
				
				if ((accessType & AccessType.Read) != 0)
					AddToHandle(handle, lockables, AccessType.Read, mode);

				if ((accessType & AccessType.Write) != 0)
					AddToHandle(handle, lockables, AccessType.Write, mode);

				return handle;
			}
		}

		public LockHandle Lock(ILockable lockable, AccessType accessType, LockingMode mode) {
			return Lock(new[] {lockable}, accessType, mode);
		}

		public LockHandle LockRead(ILockable lockable, LockingMode mode) {
			return Lock(lockable, AccessType.Read, mode);
		}

		public LockHandle LockRead(ILockable[] lockables, LockingMode mode) {
			return Lock(lockables, AccessType.Read, mode);
		}

		public LockHandle LockWrite(ILockable lockable, LockingMode mode) {
			return Lock(lockable, AccessType.Write, mode);
		}

		public LockHandle LockWrite(ILockable[] lockables, LockingMode mode) {
			return Lock(lockables, AccessType.Write, mode);
		}

		public LockHandle Lock(ILockable[] toWrite, ILockable[] toRead, LockingMode mode) {
			lock (this) {
				int lockCount = toRead.Length + toWrite.Length;
				LockHandle handle = new LockHandle(lockCount);

				AddToHandle(handle, toWrite, AccessType.Write, mode);
				AddToHandle(handle, toRead, AccessType.Read, mode);

				return handle;
			}
		}

		private LockingQueue GetQueueFor(ILockable lockable) {
			LockingQueue queue;

			if (!queuesMap.TryGetValue(lockable.RefId, out queue)) {
				queue = new LockingQueue(lockable);
				queuesMap[lockable.RefId] = queue;
			}

			return queue;
		}

		public void Unlock(LockHandle handle) {
			lock (this) {
				handle.Release();
			}
		}

		public void Reset() {
			lock (this) {
				queuesMap.Clear();
			}
		}

		public bool IsLocked(ILockable lockable) {
			lock (this) {
				LockingQueue queue;
				if (!queuesMap.TryGetValue(lockable.RefId, out queue))
					return false;

				return !queue.IsEmpty;
			}
		}
	}
}