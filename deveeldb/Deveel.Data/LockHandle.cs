// 
//  LockHandle.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Text;

using Deveel.Diagnostics;

namespace Deveel.Data {
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
		private readonly Lock[] lock_list;

		/// <summary>
		/// A temporary index used during initialisation of object to add locks.
		/// </summary>
		private int lock_index;

		/// <summary>
		/// Set when the <see cref="UnlockAll"/> method is called for the first time.
		/// </summary>
		private bool unlocked;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="lock_count">The number of locks that will be write into
		/// this handle.</param>
		internal LockHandle(int lock_count) {
			lock_list = new Lock[lock_count];
			lock_index = 0;
			unlocked = false;
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
			lock_list[lock_index] = l;
			++lock_index;
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
				for (int i = lock_list.Length - 1; i >= 0; --i) {
					lock_list[i].Release();
				}
				unlocked = true;
			}
		}

		/// <summary>
		/// Blocks until access to the given DataTable object is safe.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="access_type"></param>
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
		public void CheckAccess(DataTable table, AccessType access_type) {
			for (int i = lock_list.Length - 1; i >= 0; --i) {
				Lock l = lock_list[i];
				if (l.Table == table) {
					l.CheckAccess(access_type);
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
				Debug.Write(DebugLevel.Error, this, "Finalize released a table Lock - " +
				  "This indicates that there is a serious error.  Locks should " +
				  "only have a very short life span.  The 'UnlockAll' method should " +
				  "have been called before finalization.  " + ToString());
			}
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder str = new StringBuilder("LockHandle: ");
			for (int i = 0; i < lock_list.Length; ++i)
				str.Append(lock_list[i].ToString());
			return str.ToString();
		}
	}
}