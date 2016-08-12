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
using System.Text;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class Lock {
		private bool exclusiveMode;
		private int sharedAccess;

		internal Lock(LockingQueue queue, LockingMode mode, AccessType accessType) {
			Queue = queue;
			AccessType = accessType;
			Mode = mode;
		}

		private LockingQueue Queue { get; set; }

		public AccessType AccessType { get; private set; }

		public LockingMode Mode { get; private set; }

		private bool WasChecked { get; set; }

		internal ILockable Lockable {
			get { return Queue.Lockable; }
		}

		internal void OnAcquired() {
			StartMode();
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

		internal void Release() {
			EndMode();
			Queue.Release(this);

			if (!WasChecked) {
				Queue.Database.Context.OnWarning(String.Format("'{0}' was never checked", ToDebugString()));
			}
		}

		private string ToDebugString() {
			string objName;
			if (Lockable is IDbObject) {
				objName = ((IDbObject) Lockable).ObjectInfo.FullName.FullName;
			} else {
				objName = Lockable.ToString();
			}

			return String.Format("LOCK {0} ON {1} IN {2} MODE", AccessType.ToString().ToUpperInvariant(), objName,
				Mode.ToString().ToUpperInvariant());
		}

		internal void CheckAccess(AccessType accessType, int timeout) {
			if (AccessType != accessType)
				throw new InvalidOperationException("Access error on lock: invalid access type");

			if (!WasChecked) {
				Queue.CheckAccess(this, timeout);
				WasChecked = true;
			}
		}
	}
}