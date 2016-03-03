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
using System.Threading;

namespace Deveel.Data.Transactions {
	public sealed class Lock {
		private bool exclusiveMode;
		private int sharedAccess;

		internal Lock(LockingQueue queue, LockingMode mode, AccessType accessType) {
			Queue = queue;
			AccessType = accessType;
			Mode = mode;
			Queue.Acquire(this);
		}

		private LockingQueue Queue { get; set; }

		public AccessType AccessType { get; private set; }

		public LockingMode Mode { get; private set; }

		private bool WasChecked { get; set; }

		internal ILockable Lockable {
			get { return Queue.Lockable; }
		}

		private void StartMode() {
			lock (this) {
				// If currently in exclusive mode, block until not.

				while (exclusiveMode) {
					Monitor.Wait(this);
				}

				if (Mode == LockingMode.Exclusive) {
					// Set this thread to exclusive mode, and wait until all shared modes
					// have completed.

					exclusiveMode = true;
					while (sharedAccess > 0) {
						Monitor.Wait(this);
					}
				} else if (Mode == LockingMode.Shared) {
					// Increase the threads counter that are in shared mode.

					++sharedAccess;
				} else {
					throw new InvalidOperationException("Invalid mode");
				}
			}
		}

		private void EndMode() {
			lock (this) {
				if (Mode == LockingMode.Exclusive) {
					exclusiveMode = false;
					Monitor.PulseAll(this);
				} else if (Mode == LockingMode.Shared) {
					--sharedAccess;
					if (sharedAccess == 0 && exclusiveMode) {
						Monitor.PulseAll(this);
					} else if (sharedAccess < 0) {
						sharedAccess = 0;
						Monitor.PulseAll(this);
						throw new Exception("Too many 'Sahred Locks Release' calls");
					}
				} else {
					throw new InvalidOperationException("Invalid mode");
				}
			}
		}

		internal void Acquire() {
			StartMode();
		}

		internal void Release() {
			EndMode();
			Queue.Release(this);

			// TODO: if the lock was not check, silently report the error to the system
		}

		internal void CheckAccess(AccessType accessType) {
			if (AccessType == AccessType.Write && 
				accessType != AccessType.Write)
				throw new InvalidOperationException("Access error on Lock: Tried to Write to a non Write Lock.");

			if (!WasChecked) {
				Queue.CheckAccess(this);
				WasChecked = true;
			}
		}
	}
}