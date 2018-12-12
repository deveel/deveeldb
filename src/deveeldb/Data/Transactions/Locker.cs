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
	public class Locker : IDisposable {
		private List<LockHandle> openHandles;
		private Dictionary<object, LockingQueue> queuesMap;
		internal IContext context;

		public Locker(IContext context) {
			this.context = context;
			openHandles = new List<LockHandle>(128);
			queuesMap = new Dictionary<object, LockingQueue>();
		}

		~Locker() {
			Dispose(false);
		}

		private LockingQueue GetQueueFor(ILockable lockable) {
			LockingQueue queue;

			if (!queuesMap.TryGetValue(lockable.RefId, out queue)) {
				queue = new LockingQueue(context, lockable);
				queuesMap[lockable.RefId] = queue;
			}

			return queue;
		}

		public LockHandle Lock(ILockable lockable, AccessType accessType, LockingMode mode)
			=> Lock(new[] {lockable}, accessType, mode);

		public LockHandle Lock(ILockable[] lockables, AccessType accessType, LockingMode mode) {
			lock (this) {
				var handle = new LockHandle(this);

				for (int i = lockables.Length - 1; i >= 0; --i) {
					var lockable = lockables[i];
					var queue = GetQueueFor(lockable);

					if ((accessType & AccessType.Read) != 0)
						handle.AddLock(queue.Lock(mode, AccessType.Read));
					if ((accessType & AccessType.Write) != 0)
						handle.AddLock(queue.Lock(mode, AccessType.Write));
				}

				openHandles.Add(handle);

				return handle;
			}
		}

		internal void Release(LockHandle handle) {
			lock (this) {
				if (openHandles != null) {
					var index = openHandles.IndexOf(handle);
					if (index >= 0)
						openHandles.RemoveAt(index);
				}

				handle.ReleaseLocks();
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


		private void ReleaseAll() {
			lock (this) {
				if (openHandles != null) {
					foreach (var handle in openHandles) {
						handle.ReleaseLocks();
					}

					openHandles.Clear();
				}
			}
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				ReleaseAll();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}