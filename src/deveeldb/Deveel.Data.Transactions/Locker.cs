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
	public sealed class Locker : IDisposable {
		private Dictionary<object, LockingQueue> queuesMap = new Dictionary<object, LockingQueue>();
		private List<LockHandle> openHandles;

		public Locker(IDatabase database) {
			Database = database;
			openHandles = new List<LockHandle>();
		}

		~Locker() {
			Dispose(false);
		}

		public IDatabase Database { get; private set; }

		private void AddToHandle(LockHandle handle, ILockable[] lockables, AccessType accessType, LockingMode mode) {
			if (lockables == null)
				return;

			for (int i = lockables.Length - 1; i >= 0; --i) {
				var lockable = lockables[i];
				var queue = GetQueueFor(lockable);

				handle.AddLock(queue.NewLock(mode, accessType));
			}
		}

		private void Dispose(bool disposing) {
			lock (this) {
				if (disposing) {
					if (openHandles != null) {
						foreach (var handle in openHandles) {
							handle.Release();
						}

						openHandles.Clear();
					}

					if (queuesMap != null)
						queuesMap.Clear();
				}

				queuesMap = null;
				openHandles = null;
				Database = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
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

				openHandles.Add(handle);

				return handle;
			}
		}

		private LockingQueue GetQueueFor(ILockable lockable) {
			LockingQueue queue;

			if (!queuesMap.TryGetValue(lockable.RefId, out queue)) {
				queue = new LockingQueue(Database, lockable);
				queuesMap[lockable.RefId] = queue;
			}

			return queue;
		}

		public void Unlock(LockHandle handle) {
			lock (this) {
				if (openHandles != null) {
					var index = openHandles.IndexOf(handle);
					if (index >= 0)
						openHandles.RemoveAt(index);
				}

				handle.Release();
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

		public void CheckAccess(ILockable[] lockables, AccessType accessType, int timeout) {
			if (openHandles == null || lockables == null)
				return;

			foreach (var handle in openHandles) {
				foreach (var lockable in lockables) {
					if (handle.IsHandled(lockable))
						handle.CheckAccess(lockable, accessType, timeout);
				}
			}
		}
	}
}