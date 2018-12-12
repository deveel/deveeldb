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
using System.Threading;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Transactions {
	public sealed class Lock {
		private readonly LockingQueue queue;
		private readonly LockingMode mode;
		private bool inExclusiveMode;
		private int sharedAccessCount;
		private bool wasChecked;

		public Lock(LockingQueue queue, LockingMode mode, AccessType accessType) {
			this.queue = queue;
			this.mode = mode;
			AccessType = accessType;
		}

		internal ILockable Locked => queue.Locked;

		internal bool Released { get; set; }

		internal AccessType AccessType { get; }

		internal void Acquire() {
			lock (this) {
				// If currently in exclusive mode, block until not.

				while (inExclusiveMode) {
					Monitor.Wait(this);
				}

				if (mode == LockingMode.Exclusive) {
					// Set this thread to exclusive mode, and wait until all shared modes
					// have completed.

					inExclusiveMode = true;
					while (sharedAccessCount > 0) {
						Monitor.Wait(this);
					}
				} else if (mode == LockingMode.Shared) {
					// Increase the threads counter that are in shared mode.

					++sharedAccessCount;
				} else {
					throw new InvalidOperationException("Invalid mode");
				}
			}
		}

		internal void Release() {
			lock (this) {
				if (mode == LockingMode.Exclusive) {
					inExclusiveMode = false;
					Monitor.PulseAll(this);
				} else if (mode == LockingMode.Shared) {
					--sharedAccessCount;
					if (sharedAccessCount == 0 && inExclusiveMode) {
						Monitor.PulseAll(this);
					} else if (sharedAccessCount < 0) {
						sharedAccessCount = 0;
						Monitor.PulseAll(this);
						throw new Exception("Too many 'Sahred Locks Release' calls");
					}
				} else {
					throw new InvalidOperationException("Invalid mode");
				}
			}

			queue.Release(this);
			Released = true;

			// TODO: notify the system if the lock was ever checked
		}

		internal void Wait(AccessType type, int timeout) {
			if (AccessType != type)
				throw new InvalidOperationException("Access error on lock: invalid access type");

			if (!wasChecked) {
				queue.Wait(this, timeout);
				wasChecked = true;
			}
		}
	}
}