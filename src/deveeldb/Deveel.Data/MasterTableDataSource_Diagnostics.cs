// 
//  Copyright 2010-2011  Deveel
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

using Deveel.Data.Util;
using Deveel.Diagnostics;

namespace Deveel.Data {
	public abstract partial class MasterTableDataSource {
		/// <summary>
		/// Checks to determine if it is safe to clean up any resources in the
		/// table, and if it is safe to do so, the space is reclaimed.
		/// </summary>
		protected abstract void CheckForCleanup();

		public virtual void Repair(IUserTerminal terminal) {
			throw new NotSupportedException();
		}

		/// <summary>
		/// This is called by the 'open' method.
		/// </summary>
		/// <remarks>
		/// It performs a scan of the records and marks any rows that are 
		/// uncommitted as deleted. It also checks that the row is not within 
		/// the master index.
		/// </remarks>
		private void DoOpeningScan() {
			lock (this) {
				DateTime inTime = DateTime.Now;

				// ASSERTION: No root locks and no pending transaction changes,
				//   VERY important we assert there's no pending transactions.
				if (IsRootLocked || HasTransactionChangesPending)
					// This shouldn't happen if we are calling from 'open'.
					throw new Exception("Odd, we are root locked or have pending journal changes.");

				// This is pointless if we are in Read only mode.
				if (!IsReadOnly) {
					// A journal of index changes during this scan...
					MasterTableJournal journal = new MasterTableJournal();

					// Get the master index of rows in this table
					IIndexSet indexSet = CreateIndexSet();
					IIndex masterIndex = indexSet.GetIndex(0);

					// NOTE: We assume the index information is correct and that the
					//   allocation information is potentially bad.

					int rowCount = RawRowCount;
					for (int i = 0; i < rowCount; ++i) {
						// Is this record marked as deleted?
						if (!IsRecordDeleted(i)) {
							// Get the type flags for this record.
							RecordState type = RecordTypeInfo(i);
							// Check if this record is marked as committed removed, or is an
							// uncommitted record.
							if (type == RecordState.CommittedRemoved ||
								type == RecordState.Uncommitted) {
								// Check it's not in the master index...
								if (!masterIndex.Contains(i)) {
									// Delete it.
									DoHardRowRemove(i);
								} else {
									Debug.Write(DebugLevel.Error, this,
												  "Inconsistant: Row is indexed but marked as " +
												  "removed or uncommitted.");
									Debug.Write(DebugLevel.Error, this,
												  "Row: " + i + " Type: " + type +
												  " Table: " + TableName);
									// Mark the row as committed added because it is in the index.
									WriteRecordType(i, 0x010);

								}
							} else {
								// Must be committed added.  Check it's indexed.
								if (!masterIndex.Contains(i)) {
									// Not indexed, so data is inconsistant.
									Debug.Write(DebugLevel.Error, this,
												  "Inconsistant: Row committed added but not in master index.");
									Debug.Write(DebugLevel.Error, this,
												  "Row: " + i + " Type: " + type +
												  " Table: " + TableName);
									// Mark the row as committed removed because it is not in the
									// index.
									WriteRecordType(i, 0x020);

								}
							}
						} else {
							// if deleted
							// Check this record isn't in the master index.
							if (masterIndex.Contains(i)) {
								// It's in the master index which is wrong!  We should remake the
								// indices.
								Debug.Write(DebugLevel.Error, this, "Inconsistant: Row is removed but in index.");
								Debug.Write(DebugLevel.Error, this, "Row: " + i + " Table: " + TableName);
								// Mark the row as committed added because it is in the index.
								WriteRecordType(i, 0x010);

							}
						}
					} // for (int i = 0 ; i < row_count; ++i)

					// Dispose the index set
					indexSet.Dispose();
				}

				TimeSpan benchTime = DateTime.Now - inTime;
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								  "Opening scan for " + ToString() + " (" + TableName + ") took " +
								  benchTime + "ms.");
				}

				OnOpenScan();
			}
		}

		/// <summary>
		/// Called after an open process which scans for errors
		/// </summary>
		protected virtual void OnOpenScan() {
		}

		/// <summary>
		/// Returns an implementation of <see cref="IRawDiagnosticTable"/> that 
		/// we can use to diagnose problems with the data in this source.
		/// </summary>
		/// <returns></returns>
		internal IRawDiagnosticTable GetRawDiagnosticTable() {
			return new MRawDiagnosticTable(this);
		}

		/// <summary>
		/// A <see cref="IRawDiagnosticTable"/> implementation that provides 
		/// direct access to the root data of this table source bypassing any 
		/// indexing schemes.
		/// </summary>
		/// <remarks>
		/// This interface allows for the inspection and repair of data files.
		/// </remarks>
		private sealed class MRawDiagnosticTable : IRawDiagnosticTable {
			public MRawDiagnosticTable(MasterTableDataSource mtds) {
				this.mtds = mtds;
			}

			private readonly MasterTableDataSource mtds;

			// ---------- Implemented from IRawDiagnosticTable -----------

			public int PhysicalRecordCount {
				get {
					try {
						return mtds.RawRowCount;
					} catch (IOException e) {
						throw new ApplicationException(e.Message);
					}
				}
			}

			public DataTableDef DataTableDef {
				get { return mtds.TableInfo; }
			}

			public RecordState GetRecordState(int recordIndex) {
				try {
					return mtds.RecordTypeInfo(recordIndex);
				} catch (IOException e) {
					throw new ApplicationException(e.Message);
				}
			}

			public int GetRecordSize(int record_index) {
				return -1;
			}

			public TObject GetCellContents(int column, int record_index) {
				return mtds.GetCellContents(column, record_index);
			}

			public String GetRecordMiscInformation(int record_index) {
				return null;
			}

		}
	}
}