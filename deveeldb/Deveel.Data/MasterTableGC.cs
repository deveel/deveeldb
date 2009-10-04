//  
//  MasterTableGC.cs
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
using System.IO;

using Deveel.Data.Collections;
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
	sealed class MasterTableGC {
		/// <summary>
		/// The MasterTableDataSource that this collector is managing.
		/// </summary>
		private readonly MasterTableDataSource data_source;

		/// <summary>
		/// If this is true, then a full sweep of the table is due to reclaim all
		/// deleted rows from the table.
		/// </summary>
		private bool full_sweep_due;

		/// <summary>
		/// The list of all rows from the master table that we have been notified
		/// of being deleted.
		/// 
		/// NOTE: This list shouldn't get too large.  If it does, we should clear it
		///   and toggle the 'full_sweep_due' variable to true.
		/// </summary>
		private BlockIntegerList deleted_rows;

		// The time when the last garbage collection event occurred.
		private DateTime last_garbage_success_event;
		private DateTime last_garbage_try_event;

		internal MasterTableGC(MasterTableDataSource data_source) {
			this.data_source = data_source;
			full_sweep_due = false;
			deleted_rows = new BlockIntegerList();
			last_garbage_success_event = DateTime.Now;
			last_garbage_try_event = DateTime.MinValue;
		}

		/// <summary>
		/// Returns the IDebugLogger object that we can use to log debug messages.
		/// </summary>
		/*
		TODO:
		public IDebugLogger Debug {
			get { return data_source.Debug; }
		}
		*/

		/// <summary>
		/// Called by the <see cref="MasterTableDataSource"/> to notify the 
		/// collector that a row has been marked as committed deleted.
		/// </summary>
		/// <param name="row_index"></param>
		/// <remarks>
		/// <b>Synchronization</b> We must be synchronized over the underlying
		/// data source when this is called. (This is guarenteed if called from
		/// <see cref="MasterTableDataSource"/>).
		/// </remarks>
		public void MarkRowAsDeleted(int row_index) {
			if (full_sweep_due == false) {
				bool b = deleted_rows.UniqueInsertSort(row_index);
				if (b == false) {
					throw new ApplicationException("Row marked twice for deletion.");
				}
			}
		}

		/// <summary>
		/// Called by the <see cref="MasterTableDataSource"/> to notify the 
		/// collector to do a full sweep and remove of records in the table at 
		/// the next scheduled collection.
		/// </summary>
		/// <remarks>
		/// <b>Synchronization</b> We must be synchronized over 'data_source' 
		/// when this is called. (This is guarenteed if called from 
		/// <see cref="MasterTableDataSource"/>).
		/// </remarks>
		public void MarkFullSweep() {
			full_sweep_due = true;
			if (deleted_rows.Count > 0) {
				deleted_rows = new BlockIntegerList();
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
		internal void Collect(bool force) {

			try {
				int check_count = 0;
				int delete_count = 0;

				// Synchronize over the master data table source so no other threads
				// can interfere when we collect this information.
				lock (data_source) {

					if (data_source.IsClosed) {
						return;
					}

					// If root is locked, or has transaction changes pending, then we
					// can't delete any rows marked as deleted because they could be
					// referenced by transactions or result sets.
					if (force ||
						(!data_source.IsRootLocked &&
						 !data_source.HasTransactionChangesPending)) {

						last_garbage_success_event = DateTime.Now;
						last_garbage_try_event = DateTime.MinValue;

						// Are we due a full sweep?
						if (full_sweep_due) {
							int raw_row_count = data_source.RawRowCount;
							for (int i = 0; i < raw_row_count; ++i) {
								// Synchronized in data_source.
								bool b = data_source.HardCheckAndReclaimRow(i);
								if (b) {
									++delete_count;
								}
								++check_count;
							}
							full_sweep_due = false;
						} else {
							// Are there any rows marked as deleted?
							int size = deleted_rows.Count;
							if (size > 0) {
								// Go remove all rows marked as deleted.
								for (int i = 0; i < size; ++i) {
									int row_index = deleted_rows[i];
									// Synchronized in data_source.
									data_source.HardRemoveRow(row_index);
									++delete_count;
									++check_count;
								}
							}
							deleted_rows = new BlockIntegerList();
						}

						if (check_count > 0) {
							if (Debug.IsInterestedIn(DebugLevel.Information)) {
								Debug.Write(DebugLevel.Information, this,
										  "Row GC: [" + data_source.Name +
										  "] check_count=" + check_count +
										  " delete count=" + delete_count);
								Debug.Write(DebugLevel.Information, this,
										  "GC row sweep deleted " + delete_count + " rows.");
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