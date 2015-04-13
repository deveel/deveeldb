// 
//  Copyright 2010-2015 Deveel
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
using System.Threading;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Transactions {
	public sealed class Locker {
		private readonly Dictionary<object, LockingQueue> queuesMap = new Dictionary<object, LockingQueue>();

		public Locker(IDatabaseContext context) {
			DatabaseContext = context;
		}

		public IDatabaseContext DatabaseContext { get; private set; }

		public LockHandle Lock(ILockable[] toWrite, ILockable[] toRead, LockingMode mode) {
			// Set up the local constants.

			int lockCount = toRead.Length + toWrite.Length;
			LockHandle handle = new LockHandle(lockCount);

			lock (this) {
				Lock @lock;
				LockingQueue queue;

				// Add Read and Write locks to cache and to the handle.
				for (int i = toWrite.Length - 1; i >= 0; --i) {
					var toWriteLock = toWrite[i];
					queue = GetQueueFor(toWriteLock);

					// slightly confusing: this will add Lock to given table queue
					@lock = new Lock(queue, mode, AccessType.Write);
					@lock.Acquire();
					handle.AddLock(@lock);
				}

				for (int i = toRead.Length - 1; i >= 0; --i) {
					var toReadLock = toRead[i];
					queue = GetQueueFor(toReadLock);

					// slightly confusing: this will add Lock to given table queue
					@lock = new Lock(queue, mode, AccessType.Read);
					@lock.Acquire();
					handle.AddLock(@lock);
				}
			}

			return handle;
		}

		private LockingQueue GetQueueFor(ILockable lockable) {
			LockingQueue queue;

			// If queue not in hashtable then create a new one and write it into mapping
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