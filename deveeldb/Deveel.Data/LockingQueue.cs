//  
//  LockingQueue.cs
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
using System.Collections;
using System.Text;
using System.Threading;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// This class is used in the <see cref="LockingMechanism"/> class.
	/// </summary>
	/// <remarks>
	/// It maintains a queue of threads that have locked the table this 
	/// queue refers to.  A Lock means the table is either pending to be 
	/// accessed, or the data in the table is being used.
	/// <para>
	/// A write Lock in the queue stops any concurrently running threads from
	/// accessing the tables.  A read Lock can go ahead only if there is no write
	/// Lock in the queue below it.
	/// </para>
	/// <para>
	/// The rules are simple, and allow for reading of tables to happen concurrently
	/// and writing to happen sequentually.  Once a table is pending being written
	/// to, it must be guarenteed that no thread can Read the table while the Write
	/// is happening.
	/// </para>
	/// </remarks>
	sealed class LockingQueue {
		/// <summary>
		/// The <see cref="DataTable"/> this queue is 'protecting'
		/// </summary>
		private readonly DataTable parent_table;

		/// <summary>
		/// This is the queue that stores the table locks.
		/// </summary>
		private readonly ArrayList queue;


		internal LockingQueue(DataTable table) {
			parent_table = table;
			queue = new ArrayList();
		}

		/// <summary>
		/// Returns the DataTable object the queue is 'attached' to.
		/// </summary>
		internal DataTable Table {
			get { return parent_table; }
		}

		/// <summary>
		/// Adds a Lock to the queue.
		/// </summary>
		/// <param name="l"></param>
		/// <remarks>
		/// <b>Note</b>: This method is thread safe since it is only called 
		/// from the <see cref="LockingMechanism"/> synchronized methods.
		/// <para>
		/// <b>Thread Safety</b>: This has to be synchronized because we don't 
		/// want new locks being added while a <see cref="CheckAccess"/> is happening.
		/// </para>
		/// </remarks>
		internal void AddLock(Lock l) {
			lock (this) {
				queue.Add(l);
			}
		}

		/// <summary>
		/// Removes a Lock from the queue.
		/// </summary>
		/// <param name="l"></param>
		/// <remarks>
		/// This also does a <see cref="Monitor.PulseAll"/> to kick any threads 
		/// that might be blocking in the <see cref="CheckAccess"/> method.
		/// <para>
		/// <b>Thread Safety</b>: This has to be synchronized because we don't want 
		/// locks to be removed while a <see cref="CheckAccess"/> is happening.
		/// </para>
		/// </remarks>
		internal void RemoveLock(Lock l) {
			lock (this) {
				queue.Remove(l);
				// Notify the table that we have released a Lock from it.
				l.Table.notifyReleaseRWLock(l.Type);
				//    Console.Out.WriteLine("Removing Lock: " + Lock);
				Monitor.PulseAll(this);
			}
		}

		/// <summary>
		/// Looks at the queue and <i>blocks</i> if the access to the table by the 
		/// means specified in the Lock is allowed or not.
		/// </summary>
		/// <param name="l"></param>
		/// <remarks>
		/// The rules for determining this are as follows:
		/// <list type="number">
		/// <item>If the Lock is a READ Lock and there is a Write Lock 'infront' of 
		/// this Lock on the queue then block.</item>
		/// <item>If the Lock is a Write Lock and the Lock isn't at the front of the 
		/// queue then block.</item>
		/// <item>Retry when a Lock is released from the queue.</item>
		/// </list>
		/// </remarks>
		internal void CheckAccess(Lock l) {
			bool blocked;
			int index, i;
			Lock test_lock;

			lock (this) {

				// Error checking.  The queue must contain the Lock.
				if (!queue.Contains(l)) {
					throw new ApplicationException("Queue does not contain the given Lock");
				}

				// If 'READ'
				if (l.Type == AccessType.Read) {

					do {
						blocked = false;

						index = queue.IndexOf(l);
						for (i = index - 1; i >= 0 && blocked == false; --i) {
							test_lock = (Lock)queue[i];
							if (test_lock.Type == AccessType.Write) {
								blocked = true;
							}
						}

						if (blocked == true) {
							Debug.Write(DebugLevel.Information, this, "Blocking on Read.");
							//            Console.Out.WriteLine("READ BLOCK: " + queue);
							try {
								Monitor.Wait(this);
							} catch (ThreadInterruptedException) { }
						}

					} while (blocked == true);

				}

				// Else must be 'Write'
				else {

					do {
						blocked = false;

						index = queue.IndexOf(l);
						if (index != 0) {
							blocked = true;
							Debug.Write(DebugLevel.Information, this, "Blocking on Write.");
							//            Console.Out.WriteLine("Write BLOCK: " + queue);
							try {
								Monitor.Wait(this);
							} catch (ThreadInterruptedException) { }
						}

					} while (blocked == true);

				}

				// Notify the Lock table that we've got a Lock on it.
				l.Table.OnAddRWLock(l.Type);

			} /* lock (this) */

		}

		public override String ToString() {
			lock (this) {
				StringBuilder str = new StringBuilder("[LockingQueue]: (");
				for (int i = 0; i < queue.Count; ++i) {
					str.Append(queue[i]);
					str.Append(", ");
				}
				str.Append(")");
				return str.ToString();
			}
		}
	}
}