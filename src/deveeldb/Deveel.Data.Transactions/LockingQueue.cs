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
using System.Linq;
using System.Threading;

namespace Deveel.Data.Transactions {
	public sealed class LockingQueue {
		private readonly List<Lock> locks;

		internal LockingQueue(ILockable lockable) {
			Lockable = lockable;
			locks = new List<Lock>();
		}

		public ILockable Lockable { get; private set; }

		public bool IsEmpty {
			get {
				lock (this) {
					return !locks.Any();
				}
			}
		}

		public void Acquire(Lock @lock) {
			lock (this) {
				locks.Add(@lock);
			}
		}

		public void Release(Lock @lock) {
			lock (this) {
				locks.Remove(@lock);
				Lockable.Released(@lock);
				Monitor.PulseAll(this);
			}
		}

		internal void CheckAccess(Lock @lock) {
			lock (this) {
				// Error checking.  The queue must contain the Lock.
				if (!locks.Contains(@lock))
					throw new InvalidOperationException("Queue does not contain the given Lock");

				// If 'READ'
				bool blocked;
				int index;
				if (@lock.AccessType == AccessType.Read) {
					do {
						blocked = false;

						index = locks.IndexOf(@lock);

						int i;
						for (i = index - 1; i >= 0 && !blocked; --i) {
							var testLock = locks[i];
							if (testLock.AccessType == AccessType.Write)
								blocked = true;
						}

						if (blocked) {
							try {
								Monitor.Wait(this);
							} catch (ThreadInterruptedException) {
							}
						}
					} while (blocked);
				} else {
					do {
						blocked = false;

						index = locks.IndexOf(@lock);

						if (index != 0) {
							blocked = true;

							try {
								Monitor.Wait(this);
							} catch (ThreadInterruptedException) {
							}
						}

					} while (blocked);
				}

				// Notify the Lock table that we've got a lock on it.
				// TODO: Lock.Table.LockAcquired(Lock);
			}
		}
	}
}