// 
//  Copyright 2010  Deveel
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
		private readonly AccessType type;

		/// <summary>
		/// The table queue this Lock is 'inside'.
		/// </summary>
		private readonly LockingQueue queue;

		/// <summary>
		/// This is set to true when the <see cref="CheckAccess"/> 
		/// method is called on this Lock.
		/// </summary>
		private bool wasChecked;

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
			wasChecked = false;
			queue.AddLock(this);
		}

		/// <summary>
		/// Gets the <see cref="AccessType">access type</see> for the current Lock.
		/// </summary>
		public AccessType Type {
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

			if (!wasChecked) {
				// Prints out a warning if a Lock was released from the table queue but
				// never had 'CheckAccess' called for it.
				string tableName = queue.Table.TableName.ToString();
				debug.Write(DebugLevel.Error, this, "Lock on table '" + tableName + "' was released but never checked.  " + ToString());
				debug.WriteException(new Exception("Lock Error Dump"));
			}
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

			if (!wasChecked) {
				queue.CheckAccess(this);
				wasChecked = true;
			}
		}
	}
}