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
using System.IO;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A row garbage collector for a master table data source that manages
	/// garbage collection over a <see cref="MasterTableDataSource"/> object.
	/// </summary>
	/// <remarks>
	/// Each time a row is committed deleted from a master table, this object 
	/// is notified. When the master table has no root locks on it, then the 
	/// garbage collector can kick in and mark all deleted rows as reclaimable.
	/// </remarks>
	public sealed class MasterTableGC {
		/// <summary>
		/// The MasterTableDataSource that this collector is managing.
		/// </summary>
		private readonly MasterTableDataSource dataSource;

		/// <summary>
		/// If this is true, then a full sweep of the table is due to reclaim all
		/// deleted rows from the table.
		/// </summary>
		private bool fullSweepDue;

		/// <summary>
		/// The list of all rows from the master table that we have been notified
		/// of being deleted.
		/// 
		/// NOTE: This list shouldn't get too large.  If it does, we should clear it
		///   and toggle the 'full_sweep_due' variable to true.
		/// </summary>
		private BlockIndex deletedRows;

		// The time when the last garbage collection event occurred.
		private DateTime lastGarbageSuccessEvent;
		private DateTime lastGarbageTryEvent;

		internal MasterTableGC(MasterTableDataSource dataSource) {
			this.dataSource = dataSource;
			fullSweepDue = false;
			deletedRows = new BlockIndex();
			lastGarbageSuccessEvent = DateTime.Now;
			lastGarbageTryEvent = DateTime.MinValue;
		}

		/// <summary>
		/// Returns the IDebugLogger object that we can use to log debug messages.
		/// </summary>
		public IDebugLogger Debug {
			get { return dataSource.InternalDebug; }
		}

		/// <summary>
		/// Called by the <see cref="MasterTableDataSource"/> to notify the 
		/// collector that a row has been marked as committed deleted.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <remarks>
		/// <b>Synchronization</b> We must be synchronized over the underlying
		/// data source when this is called. (This is guarenteed if called from
		/// <see cref="MasterTableDataSource"/>).
		/// </remarks>
		internal void MarkRowAsDeleted(int rowIndex) {
			if (fullSweepDue == false) {
				if (!deletedRows.UniqueInsertSort(rowIndex))
					throw new ApplicationException("Row marked twice for deletion.");
			}
		}

		/// <summary>
		/// Called by the <see cref="MasterTableDataSource"/> to notify the 
		/// collector to do a full sweep and remove of records in the table at 
		/// the next scheduled collection.
		/// </summary>
		/// <remarks>
		/// <b>Synchronization</b> We must be synchronized over 'dataSource' 
		/// when this is called. (This is guarenteed if called from 
		/// <see cref="MasterTableDataSource"/>).
		/// </remarks>
		public void MarkFullSweep() {
			fullSweepDue = true;
			if (deletedRows.Count > 0) {
				deletedRows = new BlockIndex();
			}
		}

		/// <summary>
		/// Performs the actual garbage collection event.
		/// </summary>
		/// <param name="force">If true, then the collection event is forced 
		/// even if there are root locks or transaction changes pending. It 
		/// is only recommended that is true when the table is shut down.</param>
		/// <remarks>
		/// This is called by the CollectionEvent object. Note that it 
		/// synchronizes over the master table data source object.
		/// </remarks>
		public void Collect(bool force) {
			try {
				int checkCount = 0;
				int deleteCount = 0;

				// Synchronize over the master data table source so no other threads
				// can interfere when we collect this information.
				lock (dataSource) {
					if (dataSource.IsClosed)
						return;

					// If root is locked, or has transaction changes pending, then we
					// can't delete any rows marked as deleted because they could be
					// referenced by transactions or result sets.
					if (force ||
						(!dataSource.IsRootLocked &&
						 !dataSource.HasTransactionChangesPending)) {

						lastGarbageSuccessEvent = DateTime.Now;
						lastGarbageTryEvent = DateTime.MinValue;

						// Are we due a full sweep?
						if (fullSweepDue) {
							int rawRowCount = dataSource.RawRowCount;
							for (int i = 0; i < rawRowCount; ++i) {
								// Synchronized in dataSource.
								if (dataSource.HardCheckAndReclaimRow(i))
									++deleteCount;
								++checkCount;
							}
							fullSweepDue = false;
						} else {
							// Are there any rows marked as deleted?
							int size = deletedRows.Count;
							if (size > 0) {
								// Go remove all rows marked as deleted.
								foreach (int rowIndex in deletedRows) {
									// Synchronized in dataSource.
									dataSource.HardRemoveRow(rowIndex);
									++deleteCount;
									++checkCount;
								}
							}
							deletedRows = new BlockIndex();
						}

						if (checkCount > 0) {
							if (Debug.IsInterestedIn(DebugLevel.Information)) {
								Debug.Write(DebugLevel.Information, this,
										  "Row GC: [" + dataSource.Name +
										  "] check count=" + checkCount +
										  " delete count=" + deleteCount);
								Debug.Write(DebugLevel.Information, this,"GC row sweep deleted " + deleteCount + " rows.");
							}
						}

					} // if not roots locked and not transactions pending

				} // lock
			} catch (IOException e) {
				Debug.WriteException(e);
			}
		}
	}
}