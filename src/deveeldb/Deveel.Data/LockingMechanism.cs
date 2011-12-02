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
using System.Collections;
using System.Threading;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// This class represents a model for locking the tables in a database during
	/// any sequence of concurrent Read/Write accesses.
	/// </summary>
	/// <remarks>
	/// Every table in the database has an 'access_queue' that is generated the
	/// first time the table is accessed.  When a Read or Write request happens,
	/// the thread and the type of access is WriteByte onto the top of the queue.
	/// When the Read/Write access to the table has completed, the access is removed
	/// from the queue.
	/// <para>
	/// An access to the table may be 'blocked' until other threads have completed
	/// their access of the table.
	/// </para>
	/// <para>
	/// A table that has a 'Read Lock' can not be altered until the table object
	/// is released.  A table that has a 'Write Lock' may not be Read until the
	/// table object is released.
	/// </para>
	/// <para>
	/// The general rules are:
	/// <list type="bullet">
	/// <item>A Read request can go ahead if there are no Write request infront of
	/// this request in the access queue.</item>
	/// <item>A Write request can go ahead if the Write request is at the front of
	/// the access queue.</item>
	/// </list>
	/// <para>
	/// This class requires some for-sight to which tables will be read/written
	/// to.  We must pass all tables being read/written in a single stage.  This
	/// implies a 2 stage process, the 1st determining which tables are being
	/// accessed and the 2nd performing the actual operations.
	/// </para>
	/// <para>
	/// Some operations such as creating and dropping and modifying the security
	/// tables may require that no threads interfere with the database state while
	/// the operation is occuring.  This is handled through an <i>Excluside Mode</i>.
	/// When an object calls the locking mechanism to switch into exclusive mode, it
	/// blocks until all access to the database are complete, then continues,
	/// blocking all other threads until the exclusive mode is cancelled.
	/// </para>
	/// <para>
	/// The locking system, in simple terms, ensures that any multiple Read
	/// operations will happen concurrently, however Write operations block until
	/// all operations are complete.
	/// </para>
	/// <para>
	/// <b>Thread safety</b>: This method implements some important concurrent models
	///  for ensuring that queries can never be corrupted.
	/// </para>
	/// </remarks>
	public sealed class LockingMechanism {
		/// <summary>
		/// This <see cref="Hashtable"/> is a mapping from a 
		/// <see cref="DataTable"/> to the <see cref="LockingQueue"/> 
		/// object that is available for it.
		/// </summary>
		private readonly Hashtable queues_map = new Hashtable();

		/// <summary>
		/// This boolean is set as soon as a <see cref="Thread"/> requests 
		/// to go into 'exclusive mode'.
		/// </summary>
		private bool in_exclusive_mode = false;

		/// <summary>
		/// This contains the number of threads that have requested to go into
		/// 'shared mode'. It is incremented each time <see cref="SetMode">SetMode(Shared)</see> 
		/// is called.
		/// </summary>
		private int shared_mode = 0;

		private readonly IDebugLogger debug;

		/// <summary>
		/// 
		/// </summary>
		internal LockingMechanism(IDebugLogger logger) {
			debug = logger;
		}

		/// <summary>
		/// This is a helper function for returning the <see cref="LockingQueue"/>
		/// object for the <see cref="DataTable"/> object.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// If there has not previously been a queue instantiated for the table, 
		/// it creates a new one and adds it to the <see cref="Hashtable"/>.
		/// <para>
		/// <b>Issue</b>: Not synchronized because we guarenteed to be called from 
		/// a synchronized method right?
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private LockingQueue GetQueueFor(DataTable table) {
			LockingQueue queue = (LockingQueue)queues_map[table];

			// If queue not in hashtable then create a new one and WriteByte it into mapping
			if (queue == null) {
				queue = new LockingQueue(table);
				queues_map[table] = queue;
			}

			return queue;
		}

		/// <summary>
		/// Resets this object so it may be reused.
		/// </summary>
		/// <remarks>
		/// This will release all internal <see cref="DataTable"/> queues 
		/// that are being kept.
		/// </remarks>
		public void Reset() {
			lock (this) {
				// Check we are in exclusive mode,
				if (!IsInExclusiveMode) {
					// This is currently just a warning but should be upgraded to a
					// full error.
					debug.WriteException(new Exception("Should not clear a " +
									"LockingMechanism that's not in exclusive mode."));
				}
				queues_map.Clear();
			}

		}

		/// <summary>
		/// This method locks the given tables for either reading or writing.
		/// </summary>
		/// <param name="t_write"></param>
		/// <param name="t_read"></param>
		/// <remarks>
		/// It puts the access locks in a queue for the given tables.  This <i>reserves</i>
		/// the rights for this thread to access the table in that way.  This
		/// reservation can be used by the system to decide table accessability.
		/// <para>
		/// <b>Important Note</b>: We must ensure that a single <see cref="Thread"/> can 
		/// not create multiple table locks.  Otherwise it will cause situations where 
		/// deadlock can result.
		/// </para>
		/// <para>
		/// <b>Important Note</b>: We must ensure that once a Lock has occured, it
		/// is unlocked at a later time <i>no matter what happens</i>. Otherwise there
		/// will be situations where deadlock can result.
		/// </para>
		/// <para>
		/// <b>Note</b>: A <see cref="LockHandle"/> should not be given to another <see cref="Thread"/>.
		/// </para>
		/// <para>
		/// <b>Thread Safety</b>: This method is synchronized to ensure multiple additions 
		/// to the locking queues can happen without interference.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public LockHandle LockTables(DataTable[] t_write, DataTable[] t_read) {

			// Set up the local constants.

			int lock_count = t_read.Length + t_write.Length;
			LockHandle handle = new LockHandle(lock_count, debug);

			lock (this) {

				Lock l;
				LockingQueue queue;
				int queue_index;

				// Add Read and Write locks to cache and to the handle.

				for (int i = t_write.Length - 1; i >= 0; --i) {
					DataTable to_write_lock = t_write[i];
					queue = GetQueueFor(to_write_lock);
					// slightly confusing: this will add Lock to given table queue
					l = new Lock(AccessType.Write, queue, debug);
					handle.AddLock(l);

					debug.Write(DebugLevel.Information, this, "[LockingMechanism] Locking for Write: " + to_write_lock.TableName);
				}

				for (int i = t_read.Length - 1; i >= 0; --i) {
					DataTable to_read_lock = t_read[i];
					queue = GetQueueFor(to_read_lock);
					// slightly confusing: this will add Lock to given table queue
					l = new Lock(AccessType.Read, queue, debug);
					handle.AddLock(l);

					debug.Write(DebugLevel.Information, this, "[LockingMechanism] Locking for READ: " + to_read_lock.TableName);
				}

			}

			debug.Write(DebugLevel.Information, this, "Locked Tables");

			return handle;

		}

		/// <summary>
		/// Unlocks the tables that were previously locked by the 
		/// <see cref="LockTables"/> method.
		/// </summary>
		/// <param name="handle"></param>
		/// <remarks>
		/// It is required that this method is called after the table references made
		/// by a query are released (set to null or forgotten). This usually means
		/// <i>after</i> the result set has been written to the client.
		/// <para>
		/// <b>Thread Safety</b>: This method is synchronized so concurrent unlocking
		/// can not corrupt the queues.
		/// </para>
		/// </remarks>
		public void UnlockTables(LockHandle handle) {
			lock (this) {
				handle.UnlockAll();
			}
			debug.Write(DebugLevel.Information, this, "UnLocked Tables");
		}

		/// <summary>
		/// Returns true if we are locked into exclusive mode.
		/// </summary>
		public bool IsInExclusiveMode {
			get {
				lock (this) {
					return in_exclusive_mode;
				}
			}
		}

		/// <summary>
		/// This method <i>must</i> be called before a threads initial access 
		/// to a <see cref="Database"/> object.
		/// </summary>
		/// <param name="mode"></param>
		/// <remarks>
		/// It registers whether the preceding database accesses will be in
		/// an 'exclusive mode' or a <i>shared mode</i>. In shared mode, any number of
		/// threads are able to access the database. In exclusive, the current thread
		/// may be the only one that may access the database.
		/// On requesting exclusive mode, it blocks until exclusive mode is available.
		/// On requesting shared mode, it blocks only if currently in exclusive mode.
		/// <para>
		/// <b>Note</b>: <i>exclusive mode</i> should be used only in system maintenance 
		/// type operations such as creating and dropping tables from the database.
		/// </para>
		/// </remarks>
		public void SetMode(LockingMode mode) {
			lock (this) {
				// If currently in exclusive mode, block until not.

				while (in_exclusive_mode == true) {
					try {
						//        Console.Out.WriteLine("Waiting because in exclusive Lock.");
						Monitor.Wait(this);
						//        Console.Out.WriteLine("Finish: Waiting because in exclusive Lock.");
					} catch (ThreadInterruptedException) {
					}
				}

				if (mode == LockingMode.Exclusive) {

					// Set this thread to exclusive mode, and wait until all shared modes
					// have completed.

					in_exclusive_mode = true;
					while (shared_mode > 0) {
						try {
							//          Console.Out.WriteLine("Waiting on exclusive Lock: " + shared_mode);
							Monitor.Wait(this);
							//          Console.Out.WriteLine("Finish: Waiting on exclusive Lock: " + shared_mode);
						} catch (ThreadInterruptedException) {
						}
					}

					debug.Write(DebugLevel.Information, this, "Locked into ** EXCLUSIVE MODE **");

				} else if (mode == LockingMode.Shared) {

					// Increase the threads counter that are in shared mode.

					++shared_mode;

					debug.Write(DebugLevel.Information, this, "Locked into SHARED MODE");

				} else {
					throw new ApplicationException("Invalid mode");
				}
			}
		}

		/// <summary>
		/// This must be called when the calls to a <see cref="Database"/> object 
		/// have finished.
		/// </summary>
		/// <param name="mode"></param>
		/// <remarks>
		/// It <i>finishes</i> the mode that the locking mechanism was set into by 
		/// the call to the <see cref="SetMode"/> method.
		/// <para>
		/// <b>Note</b>: This method <b>MUST</b> be guarenteed to be called some time 
		/// after the <see cref="SetMode"/> method. Otherwise deadlock.
		/// </para>
		/// </remarks>
		public void FinishMode(LockingMode mode) {
			lock (this) {
				if (mode == LockingMode.Exclusive) {
					in_exclusive_mode = false;
					Monitor.PulseAll(this);

					debug.Write(DebugLevel.Information, this, "UnLocked from ** EXCLUSIVE MODE **");

				} else if (mode == LockingMode.Shared) {
					--shared_mode;
					if (shared_mode == 0 && in_exclusive_mode) {
						Monitor.PulseAll(this);
					} else if (shared_mode < 0) {
						shared_mode = 0;
						Monitor.PulseAll(this);
						throw new Exception("Too many 'FinishMode(Shared)' calls");
					}

					debug.Write(DebugLevel.Information, this, "UnLocked from SHARED MODE");

				} else {
					throw new ApplicationException("Invalid mode");
				}
			}
		}
	}
}