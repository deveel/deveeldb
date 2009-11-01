//  
//  Lock.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// This is a Lock on a table in the <see cref="LockingMechanism"/> class.
	/// </summary>
	/// <remarks>
	/// A new instance of this class is created whenever a new Lock for a table 
	/// is made. A <see cref="Lock"/> may be either a <see cref="AccessType.Read"/> lock 
	/// or a <see cref="AccessType.Write"/> lock. A lock is within a <see cref="LockingQueue"/> 
	/// object.
	/// </remarks>
	public sealed class Lock {
		/// <summary>
		/// This stores the type of Lock.
		/// </summary>
		private AccessType type;

		/// <summary>
		/// The table queue this Lock is 'inside'.
		/// </summary>
		private readonly LockingQueue queue;

		/// <summary>
		/// This is set to true when the <see cref="CheckAccess"/> 
		/// method is called on this Lock.
		/// </summary>
		private bool was_checked;

		private readonly IDebugLogger debug;


		/// <summary>
		/// Instantiate a new <see cref="Lock"/> object for the given access type.
		/// </summary>
		/// <param name="type">The <see cref="AccessType">access type</see> of the lock.</param>
		/// <param name="queue">The queue used to handle the access to database tables where
		/// the Lock is placed.</param>
		/// <param name="logger">A <see cref="IDebugLogger"/> used to output debug information.</param>
		/// <remarks>
		/// The construction of this object adds the Lock into the <see cref="LockingQueue"/>
		/// provided as parameter.
		/// </remarks>
		internal Lock(AccessType type, LockingQueue queue, IDebugLogger logger) {
			this.type = type;
			this.queue = queue;
			debug = logger;
			was_checked = false;
			queue.AddLock(this);
		}

		/// <summary>
		/// Gets the <see cref="AccessType">access type</see> for the current Lock.
		/// </summary>
		internal AccessType Type {
			get { return type; }
		}

		/// <summary>
		/// Returns the <see cref="DataTable"/> object this Lock is locking
		/// </summary>
		internal DataTable Table {
			get { return queue.Table; }
		}

		/// <summary>
		/// Removes this Lock from the queue.
		/// </summary>
		/// <remarks>
		/// This is called when Lock is released from the table queues.
		/// <para>
		/// <b>Note</b>: This method does not need to be synchronized because synchronization 
		/// is handled by the <see cref="LockingMechanism.UnlockTables"/> method.
		/// </para>
		/// </remarks>
		internal void Release() {
			queue.RemoveLock(this);

			if (!was_checked) {
				// Prints out a warning if a Lock was released from the table queue but
				// never had 'CheckAccess' called for it.
				string table_name = queue.Table.TableName.ToString();
				debug.Write(DebugLevel.Error, this, "Lock on table '" + table_name + "' was released but never checked.  " + ToString());
				debug.WriteException(new Exception("Lock Error Dump"));
			}
			//    else {
			//      // Notify table we released Read/Write Lock
			//      Table.notifyReleaseRWLock(type);
			//    }
		}

		/// <summary>
		/// Checks the access for this <see cref="Lock"/>.
		/// </summary>
		/// <param name="access_type">The type of access that is currently being 
		/// done to the table. If set to <see cref="AccessType.Write"/> then 
		/// <see cref="Type"/> must be <see cref="AccessType.Write"/>. If set to
		/// <see cref="AccessType.Read"/> then <see cref="Type"/> may be either 
		/// <see cref="AccessType.Read"/> or <see cref="AccessType.Write"/>.</param>
		/// <remarks>
		/// This asks the queue that contains this <see cref="Lock"/> if it 
		/// is currently safe to access the table. If it is unsafe for the 
		/// table to be accessed, then it blocks until it is safe. Therefore, 
		/// when this method returns, it is safe to access the table for this 
		/// <see cref="Lock"/>.
		/// <para>
		/// <b>Note</b>: After the first call to this method, following calls 
		/// will not block.
		/// </para>
		/// </remarks>
		internal void CheckAccess(AccessType access_type) {
			if (access_type == AccessType.Write && type != AccessType.Write)
				throw new ApplicationException("Access error on Lock: Tried to Write to a non Write Lock.");
			if (was_checked == false) {
				queue.CheckAccess(this);
				was_checked = true;
				//      // Notify table we are Read/Write locked
				//      Table.OnAddRWLock(type);
			}
		}

		/// <inheritdoc/>
		public override string ToString() {
			return "[Lock] type: " + type + "  was_checked: " + was_checked;
		}
	}
}