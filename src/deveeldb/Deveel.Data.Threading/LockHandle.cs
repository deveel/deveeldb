// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.DbSystem;
using Deveel.Diagnostics;

namespace Deveel.Data.Threading {
	/// <summary>
	/// This represents a handle for a series of locks that a query has 
	/// over the tables in a database.
	/// </summary>
	/// <remarks>
	/// It is returned by the <see cref="LockingMechanism"/> object after 
	/// the <see cref="LockingMechanism.LockTables"/> method is used.
	/// </remarks>
	public sealed class LockHandle : IDisposable {
		/// <summary>
		/// The array of <see cref="Lock"/> objects that are being used 
		/// in this locking process.
		/// </summary>
		private readonly Lock[] lockList;

		/// <summary>
		/// A temporary index used during initialisation of object to add locks.
		/// </summary>
		private int lockIndex;

		/// <summary>
		/// Set when the <see cref="UnlockAll"/> method is called for the first time.
		/// </summary>
		private bool unlocked;

		private readonly ILogger logger;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="lockCount">The number of locks that will be write into
		/// this handle.</param>
		/// <param name="logger"></param>
		internal LockHandle(int lockCount, ILogger logger) {
			lockList = new Lock[lockCount];
			lockIndex = 0;
			unlocked = false;
			this.logger = logger;
		}

		/// <summary>
		/// Adds a new Lock to the locks for this handle.
		/// </summary>
		/// <param name="l"></param>
		/// <remarks>
		/// <b>Note</b>: This method does not need to be synchronized because synchronization 
		/// is handled by the <see cref="LockingMechanism.LockTables"/> method.
		/// </remarks>
		internal void AddLock(Lock l) {
			lockList[lockIndex] = l;
			++lockIndex;
		}

		/// <summary>
		/// Unlocks all the locks in this handle.
		/// </summary>
		/// <remarks>
		/// This removes the locks from its table queue.
		/// <para>
		/// <b>Note</b>: This method does not need to be synchronized because synchronization 
		/// is handled by the <see cref="LockingMechanism.UnlockTables"/> method.
		/// </para>
		/// </remarks>
		internal void UnlockAll() {
			if (!unlocked) {
				for (int i = lockList.Length - 1; i >= 0; --i) {
					lockList[i].Release();
				}
				unlocked = true;
			}
		}

		/// <summary>
		/// Blocks until access to the given DataTable object is safe.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="accessType"></param>
		/// <remarks>
		/// It blocks using either the Read or Read/Write privs that it has 
		/// been given. Note that this method is public and is a method that 
		/// is intended to be used outside the locking mechanism.
		/// We also provide an 'access_type' field which is set to the type 
		/// of access that is happening for this check. This is either 
		/// <see cref="AccessType.Read"/> or <see cref="AccessType.Write"/>.
		/// <para>
		/// <b>Note</b>: Any call to this method after the first call should 
		/// be instantanious.
		/// </para>
		/// </remarks>
		public void CheckAccess(DataTable table, AccessType accessType) {
			for (int i = lockList.Length - 1; i >= 0; --i) {
				Lock l = lockList[i];
				if (l.Table == table) {
					l.CheckAccess(accessType);
					return;
				}
			}
			throw new Exception("The given DataTable was not found in the Lock list for this handle");
		}

		///<summary>
		/// This will call <see cref="UnlockAll"/> on GC just in case the program 
		/// did not use the <see cref="LockingMechanism.UnlockTables"/> method in 
		/// error.
		///</summary>
		/// <remarks>
		/// This should ensure the database does not deadlock.  This method is a
		/// <i>just in case</i> clause.
		/// </remarks>
		public void Dispose() {
			if (!unlocked) {
				UnlockAll();
				logger.Error(this, "Finalize released a table Lock - " +
				  "This indicates that there is a serious error.  Locks should " +
				  "only have a very short life span.  The 'UnlockAll' method should " +
				  "have been called before finalization.  " + ToString());
			}
		}
	}
}