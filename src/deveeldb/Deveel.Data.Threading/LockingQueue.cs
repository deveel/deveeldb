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
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.DbSystem;
using Deveel.Diagnostics;

namespace Deveel.Data.Threading {
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
		private readonly DataTable table;

		/// <summary>
		/// This is the queue that stores the table locks.
		/// </summary>
		private readonly List<Lock> queue;


		internal LockingQueue(DataTable table) {
			this.table = table;
			queue = new List<Lock>();
		}

		/// <summary>
		/// Returns the DataTable object the queue is 'attached' to.
		/// </summary>
		internal DataTable Table {
			get { return table; }
		}

		/// <summary>
		/// Adds a Lock to the queue.
		/// </summary>
		/// <param name="lock"></param>
		/// <remarks>
		/// <b>Note</b>: This method is thread safe since it is only called 
		/// from the <see cref="LockingMechanism"/> synchronized methods.
		/// <para>
		/// <b>Thread Safety</b>: This has to be synchronized because we don't 
		/// want new locks being added while a <see cref="CheckAccess"/> is happening.
		/// </para>
		/// </remarks>
		public void AddLock(Lock @lock) {
			lock (this) {
				queue.Add(@lock);
			}
		}

		/// <summary>
		/// Removes a Lock from the queue.
		/// </summary>
		/// <param name="lock"></param>
		/// <remarks>
		/// This also does a <see cref="Monitor.PulseAll"/> to kick any threads 
		/// that might be blocking in the <see cref="CheckAccess"/> method.
		/// <para>
		/// <b>Thread Safety</b>: This has to be synchronized because we don't want 
		/// locks to be removed while a <see cref="CheckAccess"/> is happening.
		/// </para>
		/// </remarks>
		public void RemoveLock(Lock @lock) {
			lock (this) {
				queue.Remove(@lock);
				// Notify the table that we have released a Lock from it.
				@lock.Table.OnReadWriteLockRelease(@lock.Type);
				Monitor.PulseAll(this);
			}
		}

		/// <summary>
		/// Looks at the queue and <i>blocks</i> if the access to the table by the 
		/// means specified in the Lock is allowed or not.
		/// </summary>
		/// <param name="lock"></param>
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
		internal void CheckAccess(Lock @lock) {
			lock (this) {
				// Error checking.  The queue must contain the Lock.
				if (!queue.Contains(@lock))
					throw new ApplicationException("Queue does not contain the given Lock");

				// If 'READ'
				bool blocked;
				int index;
				if (@lock.Type == AccessType.Read) {
					do {
						blocked = false;

						index = queue.IndexOf(@lock);
						int i;
						for (i = index - 1; i >= 0 && !blocked; --i) {
							Lock testLock = queue[i];
							if (testLock.Type == AccessType.Write)
								blocked = true;
						}

						if (blocked) {
							Table.Logger.Info(this, "Blocking on Read.");

							try {
								Monitor.Wait(this);
							} catch (ThreadInterruptedException) {
							}
						}

					} while (blocked);

				}

				// Else must be 'Write'
				else {

					do {
						blocked = false;

						index = queue.IndexOf(@lock);
						if (index != 0) {
							blocked = true;
							Table.Logger.Info(this, "Blocking on Write.");

							try {
								Monitor.Wait(this);
							} catch (ThreadInterruptedException) {
							}
						}

					} while (blocked);

				}

				// Notify the Lock table that we've got a Lock on it.
				@lock.Table.OnReadWriteLockEstablish(@lock.Type);

			} /* lock (this) */

		}
	}
}